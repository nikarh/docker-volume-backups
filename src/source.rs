//! Reads a live volume for rustic, keeping each archived file consistent.
//!
//! Wraps rustic's own directory walker, which describes every entry (owner,
//! mode, symlink target, xattrs), and decides how each regular file is read:
//!
//! - A file unchanged since the parent snapshot isn't read at all; rustic
//!   reuses its content from the parent.
//! - Otherwise the file is reflinked, read into memory, or copied before it
//!   is handed to rustic, and read again after `RETRY_DELAY` while it keeps
//!   changing. The snapshot records the size and times of the exact version
//!   archived.
//! - Large files that haven't changed for a while are streamed live, and a
//!   change during the read is flagged.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Cursor, Read, Seek},
    mem::MaybeUninit,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    thread,
    time::{Duration, Instant, SystemTime},
};

use log::{info, warn};
use rustic_core::{
    Excludes, LocalSource, LocalSourceFilterOptions, LocalSourceSaveOptions, ReadSource,
    ReadSourceEntry, ReadSourceOpen, RusticResult,
    jiff::Timestamp,
    repofile::{Node, NodeType},
};

use crate::format_human_size;

/// Buffer used to read source files in large blocks.
const READ_BUFFER_SIZE: usize = 2 * 1024 * 1024;
/// Files read into memory and waiting for rustic may take up to this many
/// times `RETRY_BUFFER_SIZE`. Beyond that, files are copied instead.
const MEMORY_BUFFERS: u64 = 4;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total reads of a file, including the first one.
    pub attempts: u32,
    pub delay: Duration,
    /// Time limit for all retries of one volume.
    pub budget: Duration,
    /// Files up to this size are read into memory.
    pub buffer_size: u64,
    /// Large files changed within this time are copied before reading.
    pub copy_changed_within: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            attempts: 3,
            delay: Duration::from_secs(5),
            budget: Duration::from_secs(600),
            buffer_size: 16 * 1024 * 1024,
            copy_changed_within: Duration::from_hours(1),
        }
    }
}

/// How a volume is read: reflinks, copies, and retries of files that change
/// while being read.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub scratch: Option<Scratch>,
    pub reflink: bool,
    pub retry: RetryPolicy,
    /// Runs right before a file's data is read, to simulate writes that
    /// happen during the read.
    #[cfg(test)]
    pub before_read: Option<crate::tests::Hook>,
}

/// What happened to a volume's files during a backup.
#[derive(Debug, Default)]
pub struct VolumeReport {
    pub files: usize,
    /// Files unchanged since the parent snapshot, so not read.
    pub unchanged: usize,
    /// Files archived from a reflink copy.
    pub reflinked: usize,
    /// Reads that fully copied a file into the scratch directory.
    pub copied: usize,
    /// Files that changed while being read and were read again.
    pub retried: usize,
    /// Files whose archived copy may be inconsistent.
    pub unstable: Vec<PathBuf>,
    /// Entries that could not be archived at all.
    pub skipped: usize,
}

impl VolumeReport {
    pub fn warnings(&self) -> usize {
        self.skipped + self.unstable.len()
    }
}

/// Metadata of a file in the parent snapshot, as far as rustic compares it
/// to decide whether a file changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentFile {
    pub size: u64,
    pub mtime: Option<Timestamp>,
    pub ctime: Option<Timestamp>,
    pub inode: u64,
}

impl ParentFile {
    pub fn from_node(node: &Node) -> Self {
        Self {
            size: node.meta.size,
            mtime: node.meta.mtime,
            ctime: node.meta.ctime,
            inode: node.meta.inode,
        }
    }

    /// The same rule rustic uses to reuse a file's content from the parent.
    fn matches(&self, node: &Node) -> bool {
        self.size == node.meta.size
            && self.mtime == node.meta.mtime
            && self
                .ctime
                .zip(node.meta.ctime)
                .is_none_or(|(parent, current)| parent == current)
            && (self.inode == 0 || node.meta.inode == 0 || self.inode == node.meta.inode)
    }
}

/// Files of the parent snapshot by path relative to the volume.
pub type ParentFiles = HashMap<PathBuf, ParentFile>;

struct Shared {
    root: PathBuf,
    volume: String,
    options: ReadOptions,
    parent: ParentFiles,
    memory: Arc<MemoryBudget>,
    report: Arc<Mutex<VolumeReport>>,
}

impl Shared {
    fn report(&self) -> MutexGuard<'_, VolumeReport> {
        lock(&self.report)
    }
}

/// One volume as a rustic backup source.
pub struct VolumeSource {
    local: LocalSource,
    shared: Arc<Shared>,
}

impl VolumeSource {
    pub fn new(
        root: &Path,
        volume: &str,
        options: ReadOptions,
        parent: ParentFiles,
    ) -> RusticResult<Self> {
        let local = LocalSource::new(
            LocalSourceSaveOptions::default(),
            &Excludes::default(),
            &LocalSourceFilterOptions::default(),
            &[root],
        )?;
        let memory = Arc::new(MemoryBudget::new(
            options.retry.buffer_size.saturating_mul(MEMORY_BUFFERS),
        ));
        Ok(Self {
            local,
            shared: Arc::new(Shared {
                root: root.to_owned(),
                volume: volume.to_owned(),
                options,
                parent,
                memory,
                report: Arc::default(),
            }),
        })
    }

    pub fn report(&self) -> Arc<Mutex<VolumeReport>> {
        self.shared.report.clone()
    }
}

impl ReadSource for VolumeSource {
    type Open = VolumeFile;
    type Iter = VolumeWalker;

    fn size(&self) -> RusticResult<Option<u64>> {
        Ok(None)
    }

    fn entries(&self) -> Self::Iter {
        let scratch = self
            .shared
            .options
            .scratch
            .clone()
            .map(|scratch| VolumeScratch::new(scratch, self.shared.options.reflink));
        VolumeWalker {
            inner: self.local.entries(),
            shared: self.shared.clone(),
            scratch,
            retry_time: Duration::ZERO,
            out_of_budget: false,
        }
    }
}

/// Walks a volume and makes each regular file ready to be read.
pub struct VolumeWalker {
    inner: <LocalSource as ReadSource>::Iter,
    shared: Arc<Shared>,
    scratch: Option<VolumeScratch>,
    /// Time spent waiting for and repeating reads of changed files.
    retry_time: Duration,
    out_of_budget: bool,
}

impl Iterator for VolumeWalker {
    type Item = RusticResult<ReadSourceEntry<VolumeFile>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let entry = match self.inner.next()? {
                Ok(entry) => entry,
                Err(error) => {
                    // Typically removed between listing its directory and
                    // reading its metadata.
                    warn!("skipping an entry: {}", error.display_log());
                    self.shared.report().skipped += 1;
                    continue;
                }
            };
            let ReadSourceEntry { path, node, .. } = entry;
            if node.node_type != NodeType::File {
                return Some(Ok(ReadSourceEntry {
                    path,
                    node,
                    open: None,
                }));
            }
            self.shared.report().files += 1;
            let relative = path
                .strip_prefix(&self.shared.root)
                .unwrap_or(&path)
                .to_owned();
            if self
                .shared
                .parent
                .get(&relative)
                .is_some_and(|parent| parent.matches(&node))
            {
                self.shared.report().unchanged += 1;
                // Not read, as rustic reuses the parent's content. Should
                // rustic disagree, the live file is read.
                let open = VolumeFile::Live(LiveFile {
                    source: LiveSource::Path(path.clone()),
                    size: node.meta.size,
                    relative,
                    shared: self.shared.clone(),
                });
                return Some(Ok(ReadSourceEntry {
                    path,
                    node,
                    open: Some(open),
                }));
            }
            if let Some((node, open)) = self.prepare(&path, &relative, node) {
                return Some(Ok(ReadSourceEntry {
                    path,
                    node,
                    open: Some(open),
                }));
            }
        }
    }
}

enum Outcome {
    /// Ready to be archived: from a reflink copy, from a read during which
    /// the file didn't change, or streamed live and checked while read.
    /// The metadata describes the archived version.
    Ready(VolumeFile, fs::Metadata, u64),
    /// Changed while being read. Archived anyway when this was the `last`
    /// attempt.
    Changed {
        file: Option<(VolumeFile, fs::Metadata, u64)>,
        reason: String,
    },
    /// Not archived: removed, unreadable, or no longer a regular file.
    Skipped(String),
}

impl VolumeWalker {
    /// Reads `path` until a read is stable or the attempts or the volume's
    /// retry budget run out. Returns the node describing the archived
    /// version, or `None` if the file is skipped.
    fn prepare(
        &mut self,
        path: &Path,
        relative: &Path,
        mut node: Node,
    ) -> Option<(Node, VolumeFile)> {
        let attempts = self.shared.options.retry.attempts;
        let mut attempt = 1;
        loop {
            let last = attempt >= attempts || self.out_of_budget;
            let started = Instant::now();
            let outcome = self.read_once(path, relative, last);
            if attempt > 1 {
                self.retry_time += started.elapsed();
            }
            let (open, metadata, size) = match outcome {
                Outcome::Ready(open, metadata, size) => {
                    if attempt > 1 {
                        info!(
                            "{}: archived unchanged on attempt {attempt}",
                            path.display()
                        );
                    }
                    (open, metadata, size)
                }
                Outcome::Changed {
                    file: Some(file),
                    reason,
                } => {
                    let limit = if attempts == 1 {
                        ""
                    } else if self.out_of_budget {
                        ", retry budget of the volume used up"
                    } else {
                        ", out of attempts"
                    };
                    self.unstable(relative, &format!("{reason}{limit}"));
                    file
                }
                Outcome::Changed { file: None, reason } => {
                    if attempt == 1 {
                        self.shared.report().retried += 1;
                    }
                    let delay = self.shared.options.retry.delay;
                    info!(
                        "volume {}: {}: {reason}, reading it again (attempt {} of {attempts})",
                        self.shared.volume,
                        path.display(),
                        attempt + 1
                    );
                    if self.retry_time + delay >= self.shared.options.retry.budget {
                        self.out_of_budget = true;
                    } else {
                        thread::sleep(delay);
                        self.retry_time += delay;
                    }
                    attempt += 1;
                    continue;
                }
                Outcome::Skipped(reason) => {
                    warn!("skipping {}: {reason}", path.display());
                    self.shared.report().skipped += 1;
                    return None;
                }
            };
            describe_version(&mut node, &metadata, size);
            return Some((node, open));
        }
    }

    /// Reads a file once: from a reflink copy when possible, else from
    /// memory for small files or a full copy for large ones (either archived
    /// only once a read was stable or on the `last` attempt), else streamed
    /// from the live file.
    fn read_once(&mut self, path: &Path, relative: &Path, last: bool) -> Outcome {
        // Take the metadata from the open handle, so it describes the exact
        // file being read even if the path is replaced meanwhile.
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) => return Outcome::Skipped(describe_source_error(&error)),
        };
        let before = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => return Outcome::Skipped(describe_source_error(&error)),
        };
        if !before.is_file() {
            return Outcome::Skipped("no longer a regular file".to_owned());
        }

        let volume = self.shared.volume.clone();
        if before.len() > 0
            && let Some(scratch) = &mut self.scratch
            && let Some(copy) = scratch.reflink(&file, before.len(), path, &volume)
        {
            self.shared.report().reflinked += 1;
            // The copy can't change. The metadata from before the reflink is
            // never newer than the copied data, so a later write is always
            // noticed by the next backup.
            self.before_read(path);
            let Ok(copied) = copy.metadata() else {
                return Outcome::Skipped("cannot stat reflink copy".to_owned());
            };
            return Outcome::Ready(VolumeFile::Copy(copy), before, copied.len());
        }

        self.before_read(path);
        let retry = self.shared.options.retry;
        if retry.attempts > 1 {
            let small = before.len() <= retry.buffer_size;
            if small && let Some(lease) = self.shared.memory.try_acquire(before.len()) {
                return buffered(&file, before, lease, last);
            }
            if (small || changed_within(&before, retry.copy_changed_within))
                && let Some(scratch) = &mut self.scratch
                && let Some(copy) = scratch.copy(&file, before.len(), path, &volume)
            {
                self.shared.report().copied += 1;
                return copied(&file, copy, before, last);
            }
        }

        let size = before.len();
        Outcome::Ready(
            VolumeFile::Live(LiveFile {
                source: LiveSource::Open(file, before.clone()),
                size,
                relative: relative.to_owned(),
                shared: self.shared.clone(),
            }),
            before,
            size,
        )
    }

    #[cfg_attr(not(test), allow(clippy::unused_self, unused_variables))]
    fn before_read(&self, path: &Path) {
        #[cfg(test)]
        if let Some(hook) = &self.shared.options.before_read {
            (hook.0)(path);
        }
    }

    fn unstable(&self, relative: &Path, reason: &str) {
        unstable(&self.shared, relative, reason);
    }
}

fn unstable(shared: &Shared, relative: &Path, reason: &str) {
    warn!(
        "{}: {reason}, archived copy may be inconsistent",
        shared.root.join(relative).display()
    );
    shared.report().unstable.push(relative.to_owned());
}

/// Reads a small file into memory; it is archived only if it didn't change
/// meanwhile, or if this is the `last` attempt.
fn buffered(file: &File, before: fs::Metadata, lease: MemoryLease, last: bool) -> Outcome {
    let mut data = Vec::with_capacity(usize::try_from(before.len()).unwrap_or_default());
    if let Err(error) = file.take(before.len()).read_to_end(&mut data) {
        let reason = format!("read failed ({error})");
        return if last {
            Outcome::Skipped(reason)
        } else {
            Outcome::Changed { file: None, reason }
        };
    }
    let stable = data.len() as u64 == before.len()
        && file
            .metadata()
            .is_ok_and(|after| same_version(&before, &after));
    let size = data.len() as u64;
    let open = VolumeFile::Memory(Cursor::new(data), lease);
    if stable {
        Outcome::Ready(open, before, size)
    } else {
        Outcome::Changed {
            file: last.then_some((open, before, size)),
            reason: "changed while being read".to_owned(),
        }
    }
}

/// Checks `copy`, a full copy of `file`; it is archived only if `file`
/// didn't change while it was copied, or if this is the `last` attempt.
fn copied(file: &File, copy: File, before: fs::Metadata, last: bool) -> Outcome {
    let Ok(size) = copy.metadata().map(|copied| copied.len()) else {
        return Outcome::Skipped("cannot stat copy".to_owned());
    };
    let stable = size == before.len()
        && file
            .metadata()
            .is_ok_and(|after| same_version(&before, &after));
    let open = VolumeFile::Copy(copy);
    if stable {
        Outcome::Ready(open, before, size)
    } else {
        Outcome::Changed {
            file: last.then_some((open, before, size)),
            reason: "changed while being copied".to_owned(),
        }
    }
}

/// Makes `node` describe the version of the file read with `metadata`
/// before reading, holding `size` bytes. Times from before the read are
/// never newer than the data, so a write during the read is noticed by the
/// next backup instead of being matched against this snapshot.
fn describe_version(node: &mut Node, metadata: &fs::Metadata, size: u64) {
    let mtime = timestamp(metadata.mtime(), metadata.mtime_nsec());
    node.meta.size = size;
    node.meta.mtime = mtime;
    node.meta.atime = mtime;
    node.meta.ctime = timestamp(metadata.ctime(), metadata.ctime_nsec());
    node.meta.inode = metadata.ino();
}

fn timestamp(seconds: i64, nanoseconds: i64) -> Option<Timestamp> {
    Timestamp::new(seconds, i32::try_from(nanoseconds).ok()?).ok()
}

/// A regular file ready to be read by rustic.
pub enum VolumeFile {
    /// Read into memory while it didn't change.
    Memory(Cursor<Vec<u8>>, MemoryLease),
    /// A reflink or full copy in the scratch directory, which can't change.
    Copy(File),
    /// The live file, checked for changes once it is read.
    Live(LiveFile),
}

pub struct LiveFile {
    source: LiveSource,
    /// Bytes recorded in the snapshot; exactly this many are read.
    size: u64,
    relative: PathBuf,
    shared: Arc<Shared>,
}

enum LiveSource {
    /// Opened, with its metadata from before the read.
    Open(File, fs::Metadata),
    /// Opened only if rustic reads it after all.
    Path(PathBuf),
}

impl ReadSourceOpen for VolumeFile {
    type Reader = Box<dyn Read + Send>;

    fn open(self) -> RusticResult<Self::Reader> {
        Ok(match self {
            Self::Memory(data, lease) => Box::new(MemoryReader {
                data,
                _lease: lease,
            }),
            Self::Copy(mut file) => {
                file.rewind().map_err(open_error)?;
                Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, file))
            }
            Self::Live(live) => {
                let (file, before) = match live.source {
                    LiveSource::Open(file, before) => (file, before),
                    LiveSource::Path(path) => {
                        let file = File::open(&path).map_err(open_error)?;
                        let before = file.metadata().map_err(open_error)?;
                        (file, before)
                    }
                };
                let reader = CheckedReader {
                    inner: FixedSizeReader::new(file, live.size),
                    before,
                    relative: live.relative,
                    shared: live.shared,
                    checked: false,
                };
                Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, reader))
            }
        })
    }
}

fn open_error(error: io::Error) -> Box<rustic_core::RusticError> {
    rustic_core::RusticError::with_source(
        rustic_core::ErrorKind::InputOutput,
        "Failed to open a file for reading.",
        error,
    )
}

struct MemoryReader {
    data: Cursor<Vec<u8>>,
    _lease: MemoryLease,
}

impl Read for MemoryReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.data.read(buffer)
    }
}

/// Reads a live file and flags it once read if it changed meanwhile.
struct CheckedReader {
    inner: FixedSizeReader<File>,
    before: fs::Metadata,
    relative: PathBuf,
    shared: Arc<Shared>,
    checked: bool,
}

impl Read for CheckedReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        if read == 0 && !buffer.is_empty() && !self.checked {
            self.checked = true;
            if let Some(reason) = self.inner.problem(&self.before) {
                unstable(&self.shared, &self.relative, &reason);
            }
        }
        Ok(read)
    }
}

/// Limits how much memory files read ahead of rustic take.
struct MemoryBudget {
    limit: u64,
    used: Mutex<u64>,
}

impl MemoryBudget {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            used: Mutex::new(0),
        }
    }

    /// Reserves `size` bytes, or `None` if that would exceed the limit while
    /// other files are buffered.
    fn try_acquire(self: &Arc<Self>, size: u64) -> Option<MemoryLease> {
        let mut used = lock(&self.used);
        if *used > 0 && used.saturating_add(size) > self.limit {
            return None;
        }
        *used += size;
        Some(MemoryLease {
            budget: self.clone(),
            size,
        })
    }
}

pub struct MemoryLease {
    budget: Arc<MemoryBudget>,
    size: u64,
}

impl Drop for MemoryLease {
    fn drop(&mut self) {
        *lock(&self.budget.used) -= self.size;
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Whether the file's content or metadata changed within `window`.
fn changed_within(metadata: &fs::Metadata, window: Duration) -> bool {
    let changed = SystemTime::UNIX_EPOCH
        + Duration::new(
            u64::try_from(metadata.ctime()).unwrap_or_default(),
            u32::try_from(metadata.ctime_nsec()).unwrap_or_default(),
        );
    SystemTime::now()
        .duration_since(changed)
        .map_or(true, |age| age <= window)
}

/// Whether two stats of the same file show no change in between.
fn same_version(before: &fs::Metadata, after: &fs::Metadata) -> bool {
    before.len() == after.len()
        && before.mtime() == after.mtime()
        && before.mtime_nsec() == after.mtime_nsec()
        && before.ctime() == after.ctime()
        && before.ctime_nsec() == after.ctime_nsec()
}

fn describe_source_error(error: &io::Error) -> String {
    if error.kind() == io::ErrorKind::NotFound {
        "removed during backup".to_owned()
    } else {
        format!("cannot read: {error}")
    }
}

/// A writable directory for temporary copies of files that are read while
/// they may change. On the volumes' filesystem, files are reflinked there: a
/// reflink shares the file's data instead of copying it, takes about as long
/// as a stat, and is atomic, so concurrent writes land either entirely before
/// or entirely after it. Elsewhere, files are fully copied, so a copy can be
/// checked before it's archived.
#[derive(Debug, Clone)]
pub struct Scratch {
    dir: PathBuf,
    min_free: u64,
}

impl Scratch {
    pub fn new(dir: &Path, min_free: u64) -> io::Result<Self> {
        let scratch = Self {
            dir: dir.to_owned(),
            min_free,
        };
        // Fails unless the directory exists, is writable, and supports
        // unnamed files.
        scratch.temp_file()?;
        Ok(scratch)
    }

    /// An unnamed file that disappears when it's closed, so copies never
    /// outlive the backup, even when it crashes.
    fn temp_file(&self) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_TMPFILE)
            .open(&self.dir)
    }

    /// A new temporary file, or the free space if a copy of `size` bytes
    /// would leave less than `min_free`.
    fn temp_file_with_room(&self, size: u64) -> io::Result<Result<File, u64>> {
        let file = self.temp_file()?;
        let free = available_space(&file)?;
        Ok(if free < size.saturating_add(self.min_free) {
            Err(free)
        } else {
            Ok(file)
        })
    }
}

/// Scratch directory state of one volume.
struct VolumeScratch {
    scratch: Scratch,
    reflink_enabled: bool,
    reflinked_any: bool,
    low_space_logged: bool,
}

impl VolumeScratch {
    fn new(scratch: Scratch, reflink: bool) -> Self {
        Self {
            scratch,
            reflink_enabled: reflink,
            reflinked_any: false,
            low_space_logged: false,
        }
    }

    /// Returns a reflink copy of `file`, or `None` when it can't be made.
    fn reflink(&mut self, file: &File, size: u64, path: &Path, volume: &str) -> Option<File> {
        if !self.reflink_enabled {
            return None;
        }
        // A reflinked file takes no space until the original is rewritten,
        // and then at most its size while the copy exists.
        let result = self
            .scratch
            .temp_file_with_room(size)
            .and_then(|copy| match copy {
                Ok(copy) => reflink(file, &copy).map(|()| Ok(copy)),
                Err(free) => Ok(Err(free)),
            });
        match result {
            Ok(Ok(copy)) => {
                self.reflinked_any = true;
                Some(copy)
            }
            Ok(Err(free)) => {
                self.log_low_space(free, path, volume);
                None
            }
            Err(error) if !self.reflinked_any && is_reflink_unsupported(&error) => {
                self.reflink_enabled = false;
                info!("volume {volume}: reflinks not possible ({error})");
                None
            }
            Err(error) => {
                info!("{}: reflink failed ({error})", path.display());
                None
            }
        }
    }

    /// Returns a full copy of the first `size` bytes of `file`, or `None`
    /// when it can't be made.
    fn copy(&mut self, file: &File, size: u64, path: &Path, volume: &str) -> Option<File> {
        let result = self
            .scratch
            .temp_file_with_room(size)
            .and_then(|copy| match copy {
                Ok(mut copy) => {
                    // Uses copy_file_range, so the data doesn't pass through
                    // this process.
                    io::copy(&mut file.take(size), &mut copy)?;
                    Ok(Ok(copy))
                }
                Err(free) => Ok(Err(free)),
            });
        match result {
            Ok(Ok(copy)) => Some(copy),
            Ok(Err(free)) => {
                self.log_low_space(free, path, volume);
                None
            }
            Err(error) => {
                info!(
                    "{}: copy failed ({error}), reading the live file",
                    path.display()
                );
                None
            }
        }
    }

    fn log_low_space(&mut self, free: u64, path: &Path, volume: &str) {
        if !self.low_space_logged {
            self.low_space_logged = true;
            warn!(
                "volume {volume}: only {} free in the scratch directory, reading files too \
                 large to copy with SCRATCH_MIN_FREE={} left live, starting with {}",
                format_human_size(free),
                format_human_size(self.scratch.min_free),
                path.display()
            );
        }
    }
}

/// Errors that mean the volume can't be reflinked into `SCRATCH_DIR` at all,
/// such as a different filesystem, rather than a problem with one file.
fn is_reflink_unsupported(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EXDEV | libc::EOPNOTSUPP | libc::ENOTTY | libc::ENOSYS)
    )
}

fn reflink(source: &File, target: &File) -> io::Result<()> {
    // SAFETY: both descriptors stay open for the call; FICLONE takes the
    // source descriptor as its argument and doesn't keep it.
    let result = unsafe { libc::ioctl(target.as_raw_fd(), libc::FICLONE, source.as_raw_fd()) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn available_space(file: &File) -> io::Result<u64> {
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: fstatvfs fills the struct when it returns 0.
    let stat = unsafe {
        if libc::fstatvfs(file.as_raw_fd(), stat.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        stat.assume_init()
    };
    // Narrower than u64 on some targets.
    #[allow(clippy::useless_conversion)]
    Ok(u64::from(stat.f_bavail).saturating_mul(u64::from(stat.f_frsize)))
}

/// Yields exactly `size` bytes, so the data always matches the size recorded
/// in the snapshot: a file that grew is cut at `size`, and a file that
/// shrank or failed to read part-way is padded with zeros.
pub struct FixedSizeReader<R> {
    inner: R,
    remaining: u64,
    padded: u64,
    error: Option<io::Error>,
    eof: bool,
}

impl<R> FixedSizeReader<R> {
    pub fn new(inner: R, size: u64) -> Self {
        Self {
            inner,
            remaining: size,
            padded: 0,
            error: None,
            eof: false,
        }
    }
}

impl FixedSizeReader<File> {
    /// Describes anything that went wrong while the file was read.
    fn problem(&self, before: &fs::Metadata) -> Option<String> {
        if let Some(error) = &self.error {
            return Some(format!(
                "read failed part-way ({error}), archived with {} zero byte(s) of padding",
                self.padded
            ));
        }
        if self.padded > 0 {
            return Some(format!(
                "shrank while being read, archived with {} zero byte(s) of padding",
                self.padded
            ));
        }
        let after = self.inner.metadata().ok()?;
        (!same_version(before, &after)).then(|| "changed while being read".to_owned())
    }
}

impl<R: Read> Read for FixedSizeReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let max = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        if max == 0 {
            return Ok(0);
        }
        if self.error.is_none() && !self.eof {
            match self.inner.read(&mut buffer[..max]) {
                Ok(0) => self.eof = true,
                Ok(read) => {
                    self.remaining -= read as u64;
                    return Ok(read);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => return Err(error),
                Err(error) => self.error = Some(error),
            }
        }
        buffer[..max].fill(0);
        self.remaining -= max as u64;
        self.padded += max as u64;
        Ok(max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Broken;

    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("disk error"))
        }
    }

    #[test]
    fn fixed_size_reader_pads_shrunk_and_truncates_grown_input() {
        let mut shrunk = FixedSizeReader::new(&b"abc"[..], 5);
        let mut out = Vec::new();
        shrunk.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"abc\0\0");
        assert_eq!(shrunk.padded, 2);

        let mut grown = FixedSizeReader::new(&b"abcdef"[..], 4);
        let mut out = Vec::new();
        grown.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"abcd");
        assert_eq!(grown.padded, 0);

        let mut broken = FixedSizeReader::new(Broken, 3);
        let mut out = Vec::new();
        broken.read_to_end(&mut out).unwrap();
        assert_eq!(out, [0, 0, 0]);
        assert!(broken.error.is_some());
    }

    #[test]
    fn memory_budget_admits_one_file_beyond_the_limit_at_a_time() {
        let budget = Arc::new(MemoryBudget::new(10));
        let big = budget.try_acquire(25).unwrap();
        assert!(budget.try_acquire(1).is_none());
        drop(big);
        let a = budget.try_acquire(6).unwrap();
        assert!(budget.try_acquire(6).is_none());
        let _b = budget.try_acquire(4).unwrap();
        drop(a);
        assert!(budget.try_acquire(6).is_some());
    }

    #[test]
    fn missing_scratch_dir_is_reported() {
        let temp = tempfile::tempdir().unwrap();
        assert!(Scratch::new(&temp.path().join("missing"), 0).is_err());
    }
}
