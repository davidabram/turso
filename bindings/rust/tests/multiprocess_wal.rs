use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use turso::Builder;

const MULTIPROCESS_WAL_BUILDER_P1: &str = "test_multiprocess_wal_builder_child_p1";
const MULTIPROCESS_WAL_BUILDER_P2: &str = "test_multiprocess_wal_builder_child_p2";
const CHILD_TIMEOUT: Duration = Duration::from_secs(60);

struct ChildGuard(Child);

fn multiprocess_wal_test_builder(path: &str) -> Builder {
    let builder = Builder::new_local(path).experimental_multiprocess_wal(true);

    #[cfg(target_os = "windows")]
    {
        builder.with_io("experimental_win_iocp".to_string())
    }

    #[cfg(not(target_os = "windows"))]
    {
        builder
    }
}

fn is_unsupported_multiprocess_wal_backend_error(err: &turso::Error) -> bool {
    // The public Rust binding error
    // doesn't expose a structured variant for multiprocess WAL :(
    matches!(
        err,
        turso::Error::Error(message)
            if message.contains(
                "experimental multiprocess WAL is not supported by the active IO backend",
            )
    )
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

fn drain_child_output(child: &mut Child) -> (String, String) {
    let mut out = String::new();
    let mut err = String::new();
    if let Some(mut s) = child.stdout.take() {
        let _ = s.read_to_string(&mut out);
    }
    if let Some(mut s) = child.stderr.take() {
        let _ = s.read_to_string(&mut err);
    }
    (out, err)
}

fn wait_for_file(path: &Path, label: &str, child: &mut Child) {
    let deadline = Instant::now() + CHILD_TIMEOUT;
    loop {
        if path.exists() {
            return;
        }
        if let Some(status) = child.try_wait().ok().flatten() {
            let (out, err) = drain_child_output(child);
            panic!(
                "{label} exited before signaling readiness with status {status:?}: stdout={out}; stderr={err}"
            );
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let (out, err) = drain_child_output(child);
            panic!(
                "{label} did not signal readiness within {CHILD_TIMEOUT:?}: stdout={out}; stderr={err}"
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn output_with_timeout(command: &mut Command, label: &str) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + CHILD_TIMEOUT;
    loop {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!(
                "{label} timed out after {CHILD_TIMEOUT:?}: stdout={}; stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
async fn test_multiprocess_wal_builder_allows_second_process_open() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("builder-multiprocess-wal.db");
    let ready_path = dir.path().join("p1.ready");
    let db_path_str = db_path.to_str().unwrap();

    match multiprocess_wal_test_builder(db_path_str).build().await {
        Ok(db) => drop(db),
        Err(err) if is_unsupported_multiprocess_wal_backend_error(&err) => {
            eprintln!("skipping multiprocess WAL builder test: {err}");
            return;
        }
        Err(err) => panic!("multiprocess WAL builder preflight failed: {err}"),
    }

    let current_exe = std::env::current_exe().unwrap();
    let p1 = Command::new(&current_exe)
        .arg(MULTIPROCESS_WAL_BUILDER_P1)
        .arg("--exact")
        .arg("--nocapture")
        .env("TURSO_BUILDER_MULTIPROCESS_DB_PATH", &db_path)
        .env("TURSO_BUILDER_MULTIPROCESS_READY_PATH", &ready_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut p1 = ChildGuard(p1);
    wait_for_file(&ready_path, "p1", &mut p1.0);

    let p2 = output_with_timeout(
        Command::new(&current_exe)
            .arg(MULTIPROCESS_WAL_BUILDER_P2)
            .arg("--exact")
            .arg("--nocapture")
            .env("TURSO_BUILDER_MULTIPROCESS_DB_PATH", &db_path),
        "p2",
    );

    drop(p1);

    assert!(
        p2.status.success(),
        "p2 failed: stdout={}; stderr={}",
        String::from_utf8_lossy(&p2.stdout),
        String::from_utf8_lossy(&p2.stderr)
    );

    let db = multiprocess_wal_test_builder(db_path_str)
        .build()
        .await
        .unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT COUNT(*) FROM t WHERE x = 'from P2'", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i64>(0).unwrap(), 1);
}

#[tokio::test]
#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
async fn test_multiprocess_wal_builder_child_p1() {
    let Some(db_path) = std::env::var_os("TURSO_BUILDER_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let db_path = db_path.to_str().unwrap();
    let ready_path =
        std::env::var_os("TURSO_BUILDER_MULTIPROCESS_READY_PATH").map(std::path::PathBuf::from);

    let db = multiprocess_wal_test_builder(db_path)
        .build()
        .await
        .expect("P1: build failed");
    let conn = db.connect().expect("P1: connect failed");
    conn.execute("CREATE TABLE IF NOT EXISTS t (x TEXT)", ())
        .await
        .expect("P1: create table failed");
    if let Some(path) = ready_path {
        fs::write(path, b"ready").expect("P1: failed to write ready file");
    }

    tokio::time::sleep(Duration::from_secs(120)).await;
    drop(conn);
    drop(db);
}

#[tokio::test]
#[cfg(all(any(unix, target_os = "windows"), target_pointer_width = "64"))]
async fn test_multiprocess_wal_builder_child_p2() {
    let Some(db_path) = std::env::var_os("TURSO_BUILDER_MULTIPROCESS_DB_PATH") else {
        return;
    };
    let db_path = db_path.to_str().unwrap();

    let db = multiprocess_wal_test_builder(db_path)
        .build()
        .await
        .expect("P2: build failed");
    let conn = db.connect().expect("P2: connect failed");
    conn.execute("INSERT INTO t VALUES ('from P2')", ())
        .await
        .expect("P2: insert failed");
    println!("P2: connected and inserted");
}
