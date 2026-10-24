use std::fs::OpenOptions;
use std::io::Write;

use super::*;

/// A follow of `path` that polls often.
fn tail(path: &Path) -> Tail {
    let mut tail = Tail::new(path.to_path_buf());
    tail.poll = Duration::from_millis(10);
    tail.keepalive = Duration::from_secs(5);
    tail
}

fn append(path: &Path, data: &[u8]) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(data).unwrap();
}

/// The next line, which must not be a keepalive.
async fn next(tail: &mut Tail) -> String {
    let line = tokio::time::timeout(Duration::from_secs(5), tail.next())
        .await
        .expect("a line within 5 s")
        .expect("the stream goes on");
    assert_ne!(&line[..], b"\n", "a keepalive instead of a line");
    String::from_utf8(line.to_vec()).unwrap()
}

/// Whether a follow has nothing more to send now: its next line is the
/// keepalive.
async fn quiet(tail: &mut Tail) -> bool {
    tail.keepalive = Duration::from_millis(100);
    let line = tail.next().await;
    tail.keepalive = Duration::from_secs(5);
    line.as_deref() == Some(b"\n")
}

#[tokio::test]
async fn a_follow_starts_at_the_end_and_sends_new_lines() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    append(&path, b"old one\nold two\n");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await, "the old lines are not sent");
    append(&path, b"new one\nnew two\n");
    assert_eq!(next(&mut tail).await, "new one\n");
    assert_eq!(next(&mut tail).await, "new two\n");
    assert!(quiet(&mut tail).await);
}

#[tokio::test]
async fn a_partial_line_waits_for_its_newline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    append(&path, b"");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    append(&path, b"par");
    assert!(quiet(&mut tail).await);
    append(&path, b"tial\nnext");
    assert_eq!(next(&mut tail).await, "partial\n");
    assert!(quiet(&mut tail).await);
}

#[tokio::test]
async fn a_follow_goes_on_after_a_rename_and_a_new_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    let old = dir.path().join("k3s.log.1");
    append(&path, b"before\n");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    append(&path, b"one\n");
    assert_eq!(next(&mut tail).await, "one\n");
    std::fs::rename(&path, &old).unwrap();
    append(&old, b"two\nlast of the old");
    assert_eq!(
        next(&mut tail).await,
        "two\n",
        "the old file until a new one"
    );
    assert!(quiet(&mut tail).await);
    append(&path, b"three\n");
    assert_eq!(next(&mut tail).await, "last of the old\n");
    assert_eq!(next(&mut tail).await, "three\n");
    append(&old, b"late\n");
    append(&path, b"four\n");
    assert_eq!(next(&mut tail).await, "four\n");
    assert!(quiet(&mut tail).await);
}

#[tokio::test]
async fn a_follow_starts_over_after_a_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.log");
    append(&path, b"a long line before the follow\n");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(0).unwrap();
    append(&path, b"after\n");
    assert_eq!(next(&mut tail).await, "after\n");
    append(&path, b"more\n");
    assert_eq!(next(&mut tail).await, "more\n");
}

#[tokio::test]
async fn a_file_created_later_is_read_from_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.log");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    append(&path, b"first\nsecond\n");
    assert_eq!(next(&mut tail).await, "first\n");
    assert_eq!(next(&mut tail).await, "second\n");
}

#[tokio::test]
async fn blank_lines_are_skipped_and_bad_bytes_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    append(&path, b"a\n\n\nb\ncaf\xff\n");
    assert_eq!(next(&mut tail).await, "a\n");
    assert_eq!(next(&mut tail).await, "b\n");
    assert_eq!(next(&mut tail).await, "caf\u{FFFD}\n");
    assert!(quiet(&mut tail).await);
}

#[tokio::test]
async fn a_long_line_is_split() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    let mut tail = tail(&path);
    assert!(quiet(&mut tail).await);
    let mut long = vec![b'x'; MAX_LINE + 10];
    long.push(b'\n');
    append(&path, &long);
    assert_eq!(next(&mut tail).await.len(), MAX_LINE + 1);
    assert_eq!(next(&mut tail).await, format!("{}\n", "x".repeat(10)));
    append(&path, &vec![b'y'; MAX_LINE + 5]);
    assert_eq!(next(&mut tail).await, format!("{}\n", "y".repeat(MAX_LINE)));
    assert!(quiet(&mut tail).await, "the rest waits for its newline");
    append(&path, b"\n");
    assert_eq!(next(&mut tail).await, "yyyyy\n");
}

#[tokio::test]
async fn a_quiet_follow_sends_a_blank_line() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k3s.log");
    append(&path, b"old\n");
    let mut tail = tail(&path);
    tail.keepalive = Duration::from_millis(100);
    let started = Instant::now();
    assert_eq!(&tail.next().await.unwrap()[..], b"\n");
    assert!(started.elapsed() >= Duration::from_millis(100));
    append(&path, b"new\n");
    assert_eq!(&tail.next().await.unwrap()[..], b"new\n");
}
