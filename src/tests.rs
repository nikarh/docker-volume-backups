use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use bytes::Bytes;

use super::*;
use crate::{
    source::VolumeReport,
    storage::{Entry, Files},
};

#[derive(Clone)]
pub(crate) struct Hook(pub(crate) Arc<dyn Fn(&Path) + Send + Sync>);

impl std::fmt::Debug for Hook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Hook")
    }
}

fn repo_config() -> RepoConfig {
    RepoConfig {
        password: "test".to_owned(),
        host: "test".to_owned(),
        compression_level: Some(1),
        cache_dir: None,
    }
}

fn local_repo(root: &Path) -> Repo {
    Repo::open(
        Arc::new(Backend::new(LocalFiles::new(root.to_owned()))),
        repo_config(),
    )
    .unwrap()
}

/// Every regular file under `dir` by relative path.
fn read_tree(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, files: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                let relative = path.strip_prefix(root).unwrap().display().to_string();
                files.insert(relative, fs::read(&path).unwrap());
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(dir, dir, &mut files);
    files
}

#[test]
fn parses_binary_sizes() {
    assert_eq!(parse_size("10GiB").unwrap(), 10 * 1024_u64.pow(3));
    assert_eq!(parse_size("5mb").unwrap(), 5_000_000);
}

#[test]
fn formats_human_sizes() {
    assert_eq!(format_human_size(999), "999b");
    assert_eq!(format_human_size(1_500), "1.5kb");
    assert_eq!(format_human_size(10_000), "10kb");
    assert_eq!(format_human_size(10_000_000), "10mb");
    assert_eq!(format_human_size(1_000_000_000), "1gb");
}

#[test]
fn five_field_cron_is_accepted() {
    parse_schedule("0 1 * * *").unwrap();
}

#[test]
fn parses_schedule_command_cron_argument() {
    let cli = Cli::try_parse_from([
        "docker-volume-backups",
        "schedule",
        "--backup-cron",
        "0 2 * * *",
    ])
    .unwrap();

    match cli.command {
        Command::Schedule { backup_cron } => assert_eq!(backup_cron, "0 2 * * *"),
        other => panic!("expected schedule command, got {other:?}"),
    }
}

#[test]
fn options_without_command_are_rejected() {
    let error =
        Cli::try_parse_from(["docker-volume-backups", "--volumes-root", "/tmp"]).unwrap_err();

    assert_eq!(error.kind(), clap::error::ErrorKind::MissingSubcommand);
}

#[test]
fn retention_needs_a_keep_option() {
    assert!(keep_options(None, None, None, None).is_err());
    assert_eq!(
        keep_options(Some(3), None, None, None).unwrap().keep_last,
        Some(3)
    );
}

#[test]
fn restore_all_restores_each_discovered_volume() {
    let temp = tempfile::tempdir().unwrap();
    let volumes_root = temp.path().join("volumes");
    let app_volume = volumes_root.join("app");
    let media_volume = volumes_root.join("media");

    fs::create_dir_all(app_volume.join("nested")).unwrap();
    fs::create_dir_all(&media_volume).unwrap();
    fs::write(app_volume.join("state.txt"), "old app").unwrap();
    fs::write(app_volume.join("nested").join("deep.txt"), "deep").unwrap();
    fs::write(media_volume.join("state.txt"), "old media").unwrap();
    std::os::unix::fs::symlink("state.txt", app_volume.join("link")).unwrap();

    let repo = local_repo(&temp.path().join("repo"));
    backup_all(&volumes_root, &repo, &ReadOptions::default())
        .unwrap()
        .into_result("backup")
        .unwrap();

    fs::write(app_volume.join("state.txt"), "new app").unwrap();
    fs::write(media_volume.join("state.txt"), "new media").unwrap();
    fs::remove_dir_all(app_volume.join("nested")).unwrap();

    restore_all(&volumes_root, &repo).unwrap();

    assert_eq!(
        fs::read_to_string(app_volume.join("state.txt")).unwrap(),
        "old app"
    );
    assert_eq!(
        fs::read_to_string(app_volume.join("nested").join("deep.txt")).unwrap(),
        "deep"
    );
    assert_eq!(
        fs::read_link(app_volume.join("link")).unwrap(),
        Path::new("state.txt")
    );
    assert_eq!(
        fs::read_to_string(media_volume.join("state.txt")).unwrap(),
        "old media"
    );
}

#[test]
fn unchanged_data_is_stored_once_and_unchanged_files_are_not_read() {
    let temp = tempfile::tempdir().unwrap();
    let volumes_root = temp.path().join("volumes");
    // Two volumes holding the same 3 MB of data.
    let payload = (0..750_000u32)
        .flat_map(|n| n.wrapping_mul(2_654_435_761).to_le_bytes())
        .collect::<Vec<_>>();
    for volume in ["a", "b"] {
        fs::create_dir_all(volumes_root.join(volume)).unwrap();
        fs::write(volumes_root.join(volume).join("big.bin"), &payload).unwrap();
    }
    let repo = local_repo(&temp.path().join("repo"));
    let (first, _) = repo
        .backup(&volumes_root.join("a"), "a", &ReadOptions::default())
        .unwrap();
    let (copy, _) = repo
        .backup(&volumes_root.join("b"), "b", &ReadOptions::default())
        .unwrap();
    assert!(first.summary.unwrap().data_added >= payload.len() as u64);
    assert_eq!(copy.summary.unwrap().data_added_files, 0);

    let (before_read, read_count) = change_during_read("big.bin", 0, |_| {});
    let options = ReadOptions {
        before_read,
        ..ReadOptions::default()
    };
    let (again, report) = repo.backup(&volumes_root.join("a"), "a", &options).unwrap();
    assert_eq!(reads(&read_count), 0);
    assert_eq!((report.files, report.unchanged), (1, 1));
    assert_eq!(again.summary.unwrap().data_added_files, 0);
    assert_eq!(again.parent, Some(first.id));
}

#[test]
fn changed_file_is_read_again_on_the_next_backup() {
    let temp = tempfile::tempdir().unwrap();
    let app = app_volume(temp.path(), &[("state", b"one"), ("other", b"same")]);
    let repo = local_repo(&temp.path().join("repo"));
    repo.backup(&app, "app", &ReadOptions::default()).unwrap();
    fs::write(app.join("state"), b"two, longer").unwrap();
    let (_, report) = repo.backup(&app, "app", &ReadOptions::default()).unwrap();
    assert_eq!((report.files, report.unchanged), (2, 1));

    let target = temp.path().join("restored");
    repo.restore("app", &target, None).unwrap();
    assert_eq!(fs::read(target.join("state")).unwrap(), b"two, longer");
}

#[test]
fn restore_picks_a_snapshot_by_id_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let app = app_volume(temp.path(), &[("state", b"one")]);
    let repo = local_repo(&temp.path().join("repo"));
    let (first, _) = repo.backup(&app, "app", &ReadOptions::default()).unwrap();
    fs::write(app.join("state"), b"two").unwrap();
    repo.backup(&app, "app", &ReadOptions::default()).unwrap();

    let target = temp.path().join("restored");
    let prefix = first.id.to_string()[..8].to_owned();
    repo.restore("app", &target, Some(&prefix)).unwrap();
    assert_eq!(fs::read(target.join("state")).unwrap(), b"one");
}

#[test]
fn reopening_uses_the_existing_repository() {
    let temp = tempfile::tempdir().unwrap();
    let app = app_volume(temp.path(), &[("state", b"one")]);
    local_repo(&temp.path().join("repo"))
        .backup(&app, "app", &ReadOptions::default())
        .unwrap();
    assert_eq!(
        local_repo(&temp.path().join("repo"))
            .snapshots("app")
            .unwrap()
            .len(),
        1
    );
    let mut wrong = repo_config();
    wrong.password = "wrong".to_owned();
    assert!(
        Repo::open(
            Arc::new(Backend::new(LocalFiles::new(temp.path().join("repo")))),
            wrong
        )
        .is_err()
    );
}

#[test]
fn cleanup_keeps_the_newest_snapshots_and_frees_the_rest() {
    let temp = tempfile::tempdir().unwrap();
    let app = app_volume(temp.path(), &[]);
    let repo_dir = temp.path().join("repo");
    let store = Store::Local(Arc::new(Backend::new(LocalFiles::new(repo_dir.clone()))));
    let repo = local_repo(&repo_dir);
    for version in 0..3 {
        fs::write(app.join("state"), format!("version {version}")).unwrap();
        repo.backup(&app, "app", &ReadOptions::default()).unwrap();
    }
    fs::write(
        repo_dir.join("data").join("00").join("leftover.1-0.tmp"),
        "x",
    )
    .unwrap();

    cleanup(
        &store,
        &repo,
        &["app".to_owned()],
        &keep_options(Some(2), None, None, None).unwrap(),
    )
    .unwrap();
    assert_eq!(repo.snapshots("app").unwrap().len(), 2);
    assert!(
        !repo_dir
            .join("data")
            .join("00")
            .join("leftover.1-0.tmp")
            .exists()
    );

    let target = temp.path().join("restored");
    repo.restore("app", &target, None).unwrap();
    assert_eq!(fs::read(target.join("state")).unwrap(), b"version 2");
}

#[test]
fn unreadable_file_is_skipped_with_warning() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let app = app_volume(
        temp.path(),
        &[("good.txt", b"good"), ("secret.txt", b"secret")],
    );
    fs::set_permissions(app.join("secret.txt"), fs::Permissions::from_mode(0o000)).unwrap();
    if File::open(app.join("secret.txt")).is_ok() {
        // Running as root: permissions cannot make the file unreadable.
        return;
    }

    let (report, files) = backup_app(temp.path(), &ReadOptions::default());
    assert_eq!(report.skipped, 1);
    assert_eq!(files.get("good.txt").map(Vec::as_slice), Some(&b"good"[..]));
    assert!(!files.contains_key("secret.txt"));
}

/// Local files that fail writes of data packs while `fail` is set.
struct FlakyFiles {
    inner: LocalFiles,
    fail: Arc<AtomicBool>,
}

impl Files for FlakyFiles {
    fn location(&self) -> String {
        self.inner.location()
    }
    fn read(&self, path: &str, range: Option<(u64, usize)>) -> io::Result<Vec<u8>> {
        self.inner.read(path, range)
    }
    fn write(&self, path: &str, content: &[Bytes]) -> io::Result<()> {
        if self.fail.load(Ordering::SeqCst) && path.starts_with("data/") {
            return Err(io::Error::other("storage went away"));
        }
        self.inner.write(path, content)
    }
    fn list(&self, dir: &str) -> io::Result<Vec<Entry>> {
        self.inner.list(dir)
    }
    fn create_dir_all(&self, dir: &str) -> io::Result<()> {
        self.inner.create_dir_all(dir)
    }
    fn remove(&self, path: &str) -> io::Result<()> {
        self.inner.remove(path)
    }
}

#[test]
fn failing_volume_does_not_stop_other_volumes() {
    let temp = tempfile::tempdir().unwrap();
    let volumes_root = temp.path().join("volumes");
    for volume in ["a", "bad", "c"] {
        fs::create_dir_all(volumes_root.join(volume)).unwrap();
        fs::write(volumes_root.join(volume).join("data.bin"), volume).unwrap();
    }

    let fail = Arc::new(AtomicBool::new(false));
    let repo = Repo::open(
        Arc::new(Backend::new(FlakyFiles {
            inner: LocalFiles::new(temp.path().join("repo")),
            fail: fail.clone(),
        })),
        repo_config(),
    )
    .unwrap();
    // Pack uploads fail while volume "bad" is read and written, but its
    // snapshot could still be saved: it must not be, as its data is missing.
    let flag = fail.clone();
    let options = ReadOptions {
        before_read: Some(Hook(Arc::new(move |path: &Path| {
            flag.store(
                path.parent().is_some_and(|dir| dir.ends_with("bad")),
                Ordering::SeqCst,
            );
        }))),
        ..ReadOptions::default()
    };
    let results = backup_all(&volumes_root, &repo, &options).unwrap();
    assert_eq!(results.succeeded, ["a", "c"]);
    assert_eq!(results.failed, ["bad"]);
    assert!(repo.snapshots("bad").unwrap().is_empty());
    assert_eq!(repo.snapshots("a").unwrap().len(), 1);
    assert!(results.into_result("backup").is_err());
}

/// Retries without waiting; files over 1 KiB are streamed.
fn fast_retry(attempts: u32) -> RetryPolicy {
    RetryPolicy {
        attempts,
        delay: Duration::ZERO,
        budget: Duration::from_secs(60),
        buffer_size: 1024,
        copy_changed_within: Duration::MAX,
    }
}

/// Runs `change` for `name` on the first `times` reads of it and counts
/// all reads of it.
fn change_during_read(
    name: &'static str,
    times: usize,
    change: impl Fn(&Path) + Send + Sync + 'static,
) -> (Option<Hook>, Arc<AtomicUsize>) {
    let reads = Arc::new(AtomicUsize::new(0));
    let counter = reads.clone();
    let hook = Hook(Arc::new(move |path: &Path| {
        if path.file_name().is_some_and(|file| file == name) {
            let read = counter.fetch_add(1, Ordering::SeqCst);
            if read < times {
                change(path);
            }
        }
    }));
    (Some(hook), reads)
}

fn reads(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::SeqCst)
}

/// Backs up `<root>/volumes/app` into a new repository under `root`,
/// restores the snapshot, and returns the report and the restored files.
fn backup_app(root: &Path, options: &ReadOptions) -> (VolumeReport, BTreeMap<String, Vec<u8>>) {
    let repo = local_repo(&root.join("repo"));
    let (_, report) = repo
        .backup(&root.join("volumes").join("app"), "app", options)
        .unwrap();
    let target = root.join("restored");
    repo.restore("app", &target, None).unwrap();
    (report, read_tree(&target))
}

fn app_volume(root: &Path, files: &[(&str, &[u8])]) -> PathBuf {
    let app = root.join("volumes").join("app");
    fs::create_dir_all(&app).unwrap();
    for (name, data) in files {
        fs::write(app.join(name), data).unwrap();
    }
    app
}

fn content<'a>(files: &'a BTreeMap<String, Vec<u8>>, name: &str) -> Option<&'a [u8]> {
    files.get(name).map(Vec::as_slice)
}

#[test]
fn streamed_file_that_changed_is_archived_once_and_flagged() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(
        temp.path(),
        &[("hot.bin", &[b'a'; 4096]), ("cold.txt", b"cold")],
    );
    let (before_read, read_count) = change_during_read("hot.bin", 1, |path| {
        fs::write(path, [b'b'; 4096]).unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(3),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    // A streamed file is read once: it's only known to have changed after
    // its data went to rustic.
    assert_eq!(reads(&read_count), 1);
    assert_eq!((report.files, report.retried), (2, 0));
    assert_eq!(report.unstable, [PathBuf::from("hot.bin")]);
    assert_eq!(content(&files, "hot.bin").map(<[u8]>::len), Some(4096));
    assert_eq!(content(&files, "cold.txt"), Some(&b"cold"[..]));
}

/// Options that fully copy files over 1 KiB into a scratch directory
/// under `root`, without reflinks.
fn copying(root: &Path, attempts: u32, before_read: Option<Hook>) -> ReadOptions {
    let dir = root.join("scratch");
    fs::create_dir(&dir).unwrap();
    ReadOptions {
        scratch: Some(Scratch::new(&dir, 0).unwrap()),
        retry: fast_retry(attempts),
        before_read,
        ..ReadOptions::default()
    }
}

#[test]
fn large_file_that_changed_while_copied_is_copied_again() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("hot.bin", &[b'a'; 4096])]);
    let (before_read, read_count) = change_during_read("hot.bin", 1, |path| {
        fs::write(path, [b'b'; 4096]).unwrap();
    });
    let options = copying(temp.path(), 3, before_read);

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 2);
    assert_eq!((report.retried, report.copied), (1, 2));
    assert_eq!(report.unstable, Vec::<PathBuf>::new());
    assert_eq!(content(&files, "hot.bin"), Some(&[b'b'; 4096][..]));
    // Copies are unnamed and gone once read.
    assert_eq!(
        fs::read_dir(temp.path().join("scratch")).unwrap().count(),
        0
    );
}

#[test]
fn large_file_that_keeps_changing_is_archived_after_the_last_copy() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("hot.bin", &[b'a'; 4096])]);
    let (before_read, read_count) = change_during_read("hot.bin", usize::MAX, |path| {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"x").unwrap();
    });
    let options = copying(temp.path(), 3, before_read);

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 3);
    assert_eq!(report.unstable, [PathBuf::from("hot.bin")]);
    // The last copy, taken before its byte was appended.
    let restored = content(&files, "hot.bin").unwrap();
    assert_eq!(restored.len(), 4098);
    assert!(restored.starts_with(&[b'a'; 4096]));
}

#[test]
fn large_file_not_changed_recently_is_streamed_without_copying() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("old.bin", &[b'a'; 4096])]);
    let mut options = copying(temp.path(), 3, None);
    options.retry.copy_changed_within = Duration::ZERO;

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(report.copied, 0);
    assert_eq!(content(&files, "old.bin"), Some(&[b'a'; 4096][..]));
}

#[test]
fn file_that_grows_too_big_to_buffer_is_streamed_on_retry() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("db", b"small")]);
    let (before_read, read_count) = change_during_read("db", 1, |path| {
        fs::write(path, [b'b'; 4096]).unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(3),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 2);
    assert_eq!(report.unstable, Vec::<PathBuf>::new());
    assert_eq!(content(&files, "db"), Some(&[b'b'; 4096][..]));
}

#[test]
fn small_file_is_archived_after_a_stable_read() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("db.sqlite", b"version 1")]);
    let (before_read, read_count) = change_during_read("db.sqlite", 1, |path| {
        fs::write(path, b"version 2").unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(3),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 2);
    assert_eq!(report.retried, 1);
    assert_eq!(report.unstable, Vec::<PathBuf>::new());
    assert_eq!(content(&files, "db.sqlite"), Some(&b"version 2"[..]));
}

#[test]
fn file_that_keeps_changing_is_archived_after_the_last_attempt() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("log.txt", b"x")]);
    let (before_read, read_count) = change_during_read("log.txt", usize::MAX, |path| {
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(b"x").unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(3),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 3);
    assert_eq!(report.unstable, [PathBuf::from("log.txt")]);
    assert_eq!(report.warnings(), 1);
    // The size recorded before the last read, so the restored file matches
    // the snapshot.
    assert_eq!(content(&files, "log.txt"), Some(&b"xxx"[..]));
}

#[test]
fn retry_budget_limits_attempts() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("log.txt", b"x")]);
    let (before_read, read_count) = change_during_read("log.txt", usize::MAX, |path| {
        fs::write(path, b"changed").unwrap();
    });
    let options = ReadOptions {
        retry: RetryPolicy {
            budget: Duration::ZERO,
            ..fast_retry(10)
        },
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 2);
    assert_eq!(report.unstable.len(), 1);
    assert!(files.contains_key("log.txt"));
}

#[test]
fn changed_file_removed_before_retry_is_left_out() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("db-journal", b"journal"), ("db", b"db")]);
    let (before_read, _) = change_during_read("db-journal", 1, |path| {
        fs::write(path, b"half-written").unwrap();
        fs::remove_file(path).unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(3),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert!(!files.contains_key("db-journal"));
    assert_eq!(content(&files, "db"), Some(&b"db"[..]));
    assert_eq!(report.unstable, Vec::<PathBuf>::new());
    assert_eq!(report.skipped, 1);
}

#[test]
fn single_attempt_archives_changed_file_once() {
    let temp = tempfile::tempdir().unwrap();
    app_volume(temp.path(), &[("db", b"v1")]);
    let (before_read, read_count) = change_during_read("db", usize::MAX, |path| {
        fs::write(path, b"v2").unwrap();
    });
    let options = ReadOptions {
        retry: fast_retry(1),
        before_read,
        ..ReadOptions::default()
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(reads(&read_count), 1);
    assert_eq!(report.retried, 0);
    assert_eq!(report.unstable, [PathBuf::from("db")]);
    assert!(files.contains_key("db"));
}

/// A directory on the same filesystem as the build, which unlike /tmp
/// usually supports reflinks on developer machines.
fn build_dir_tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(concat!(env!("CARGO_MANIFEST_DIR"), "/target")).unwrap()
}

#[test]
fn reflinked_copy_is_unaffected_by_writes_during_read() {
    let temp = build_dir_tempdir();
    app_volume(
        temp.path(),
        &[("big.bin", &[b'a'; 8192]), ("small", b"old")],
    );
    let reflink_dir = temp.path().join("reflinks");
    fs::create_dir(&reflink_dir).unwrap();
    let (before_read, read_count) = change_during_read("big.bin", usize::MAX, |path| {
        fs::write(path, [b'b'; 100]).unwrap();
    });
    let options = ReadOptions {
        scratch: Some(Scratch::new(&reflink_dir, 0).unwrap()),
        reflink: true,
        retry: fast_retry(3),
        before_read,
    };

    let (report, files) = backup_app(temp.path(), &options);
    if report.reflinked == 0 {
        eprintln!(
            "skipping: {} does not support reflinks",
            temp.path().display()
        );
        return;
    }
    assert_eq!(reads(&read_count), 1);
    assert_eq!(report.reflinked, 2);
    assert_eq!(report.retried, 0);
    assert_eq!(report.unstable, Vec::<PathBuf>::new());
    assert_eq!(content(&files, "big.bin"), Some(&[b'a'; 8192][..]));
    // Copies are unnamed and gone once read.
    assert_eq!(fs::read_dir(&reflink_dir).unwrap().count(), 0);
}

#[test]
fn low_free_space_falls_back_to_live_reads_with_retries() {
    let temp = build_dir_tempdir();
    app_volume(temp.path(), &[("small.bin", &[b'a'; 512])]);
    let reflink_dir = temp.path().join("reflinks");
    fs::create_dir(&reflink_dir).unwrap();
    let (before_read, _) = change_during_read("small.bin", 1, |path| {
        fs::write(path, [b'b'; 512]).unwrap();
    });
    let options = ReadOptions {
        scratch: Some(Scratch::new(&reflink_dir, u64::MAX / 2).unwrap()),
        reflink: true,
        retry: fast_retry(3),
        before_read,
    };

    let (report, files) = backup_app(temp.path(), &options);
    assert_eq!(report.reflinked, 0);
    assert_eq!(report.retried, 1);
    assert_eq!(content(&files, "small.bin"), Some(&[b'b'; 512][..]));
}
