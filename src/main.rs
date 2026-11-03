use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    mem::MaybeUninit,
    net::TcpStream,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    str::FromStr,
    thread,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local, Utc};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use cron::Schedule;
use filetime::FileTime;
use log::{error, info, warn};
use ssh2::{FileStat, Session, Sftp};
use tar::{Archive, Builder, EntryType, Header, HeaderMode};
use url::Url;
use walkdir::WalkDir;
use xz2::{read::XzDecoder, write::XzEncoder};

const DEFAULT_VOLUMES_ROOT: &str = "/volumes";
const DEFAULT_LOCAL_STORAGE_ROOT: &str = "/backups";
const DEFAULT_BACKUP_CRON: &str = "0 1 * * *";
const DEFAULT_SFTP_PASSWORD_FILE: &str = "/run/secrets/SFTP_PASSWORD";
const DEFAULT_SFTP_PRIVATE_KEY_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY";
const DEFAULT_SFTP_PRIVATE_KEY_PASSPHRASE_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY_PASSPHRASE";
const DEFAULT_SCRATCH_DIR: &str = "/scratch";
/// Buffer between the archive encoder and storage. Large writes let libssh2
/// pipeline many SFTP packets per round trip instead of waiting for an ack
/// after every few kilobytes.
const STORAGE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
/// Buffer used to read source files and archives in large blocks.
const READ_BUFFER_SIZE: usize = 2 * 1024 * 1024;
/// Suffix of an archive that is still being written. It is renamed to its
/// final name only after the whole stream was written successfully, so an
/// interrupted backup never shows up as the latest archive.
const PARTIAL_SUFFIX: &str = ".partial";

#[derive(Debug, Parser)]
#[command(author, version, about, subcommand_required = true)]
struct Cli {
    #[arg(long, env = "VOLUMES_ROOT", default_value = DEFAULT_VOLUMES_ROOT)]
    volumes_root: PathBuf,

    #[arg(long, env = "STORAGE_DRIVER", default_value = "local")]
    storage_driver: StorageDriverKind,

    #[arg(
        long,
        env = "LOCAL_STORAGE_ROOT",
        default_value = DEFAULT_LOCAL_STORAGE_ROOT
    )]
    local_storage_root: PathBuf,

    #[arg(long, env = "SFTP_URL")]
    sftp_url: Option<String>,

    #[arg(long, env = "SFTP_PASSWORD")]
    sftp_password: Option<String>,

    #[arg(
        long,
        env = "SFTP_PASSWORD_FILE",
        default_value = DEFAULT_SFTP_PASSWORD_FILE
    )]
    sftp_password_file: PathBuf,

    #[arg(long, env = "SFTP_PRIVATE_KEY")]
    sftp_private_key: Option<String>,

    #[arg(
        long,
        env = "SFTP_PRIVATE_KEY_FILE",
        default_value = DEFAULT_SFTP_PRIVATE_KEY_FILE
    )]
    sftp_private_key_file: PathBuf,

    #[arg(long, env = "SFTP_PRIVATE_KEY_PASSPHRASE")]
    sftp_private_key_passphrase: Option<String>,

    #[arg(
        long,
        env = "SFTP_PRIVATE_KEY_PASSPHRASE_FILE",
        default_value = DEFAULT_SFTP_PRIVATE_KEY_PASSPHRASE_FILE
    )]
    sftp_private_key_passphrase_file: PathBuf,

    /// Archive compression for new backups. Restores detect it from the file extension.
    #[arg(long, env = "COMPRESSION", default_value = "zstd")]
    compression: CompressionKind,

    /// Compression level; defaults to 3 for zstd and 6 for xz.
    #[arg(long, env = "COMPRESSION_LEVEL")]
    compression_level: Option<u32>,

    #[arg(long, env = "RETENTION_POLICY", default_value = "count")]
    retention_policy: RetentionPolicyKind,

    #[arg(long, env = "RETENTION_COUNT", default_value_t = 7)]
    retention_count: usize,

    #[arg(long, env = "RETENTION_MIN_COUNT", default_value_t = 2)]
    retention_min_count: usize,

    #[arg(
        long,
        env = "RETENTION_MAX_TOTAL_SIZE",
        default_value = "10GiB",
        value_parser = parse_size
    )]
    retention_max_total_size: u64,

    /// Archive each file from a reflink copy (btrfs, XFS), so it can't change while it is read.
    #[arg(long, env = "REFLINK", default_value = "auto")]
    reflink: ReflinkMode,

    /// Writable directory for temporary copies of files: reflinks when it's
    /// on the volumes' filesystem and that supports them, else full copies of
    /// files too large to read into memory.
    #[arg(long, env = "SCRATCH_DIR", default_value = DEFAULT_SCRATCH_DIR)]
    scratch_dir: PathBuf,

    /// Free space that must remain in `SCRATCH_DIR` after copying a file, or
    /// after a reflinked file were fully rewritten while its copy exists.
    #[arg(
        long,
        env = "SCRATCH_MIN_FREE",
        default_value = "2GiB",
        value_parser = parse_size
    )]
    scratch_min_free: u64,

    /// How many times a small file (see `retry_buffer_size`) that changes
    /// while being read is read before its last copy is archived as possibly
    /// inconsistent. 1 disables retries.
    #[arg(long, env = "RETRY_ATTEMPTS", default_value_t = 3)]
    retry_attempts: u32,

    /// Wait before each retry pass over a volume's changed files.
    #[arg(
        long,
        env = "RETRY_DELAY",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    retry_delay: Duration,

    /// Time limit for all retries of one volume.
    #[arg(
        long,
        env = "RETRY_VOLUME_BUDGET",
        default_value = "10m",
        value_parser = humantime::parse_duration
    )]
    retry_volume_budget: Duration,

    /// Files up to this size that aren't reflinked are read into memory, and
    /// larger ones are copied into `SCRATCH_DIR`. Either way a file is
    /// archived only once a read was stable, or on the last attempt.
    #[arg(
        long,
        env = "RETRY_BUFFER_SIZE",
        default_value = "16MiB",
        value_parser = parse_size
    )]
    retry_buffer_size: u64,

    /// Files too large to read into memory are fully copied into
    /// `SCRATCH_DIR` before reading only if they changed within this time.
    /// Older ones are streamed directly, which avoids rewriting large files
    /// that rarely change.
    #[arg(
        long,
        env = "COPY_CHANGED_WITHIN",
        default_value = "1h",
        value_parser = humantime::parse_duration
    )]
    copy_changed_within: Duration,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, ValueEnum)]
enum StorageDriverKind {
    Local,
    Sftp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum CompressionKind {
    Zstd,
    Xz,
}

impl CompressionKind {
    const ALL: [Self; 2] = [Self::Zstd, Self::Xz];

    fn extension(self) -> &'static str {
        match self {
            Self::Zstd => ".tar.zst",
            Self::Xz => ".tar.xz",
        }
    }

    fn from_archive_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|kind| name.ends_with(kind.extension()))
    }
}

#[derive(Debug, Clone, Copy)]
struct Compression {
    kind: CompressionKind,
    level: u32,
}

impl Compression {
    fn from_config(config: &Cli) -> Self {
        let level = config
            .compression_level
            .unwrap_or(match config.compression {
                CompressionKind::Zstd => 3,
                CompressionKind::Xz => 6,
            });
        Self {
            kind: config.compression,
            level,
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            kind: CompressionKind::Zstd,
            level: 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ReflinkMode {
    /// Reflink when `SCRATCH_DIR` is usable and the filesystem supports it.
    Auto,
    /// Always read the live files.
    Off,
}

/// How a volume is read: compression, reflinks, and retries of files that
/// change while being read.
#[derive(Debug, Default)]
struct BackupOptions {
    compression: Compression,
    scratch: Option<Scratch>,
    reflink: bool,
    retry: RetryPolicy,
    /// Runs right before a file's data is read, to simulate writes that
    /// happen during the read.
    #[cfg(test)]
    before_read: Option<tests::Hook>,
}

impl BackupOptions {
    fn from_config(config: &Cli) -> Self {
        let reflink = config.reflink == ReflinkMode::Auto;
        let scratch = match Scratch::new(&config.scratch_dir, config.scratch_min_free) {
            Ok(scratch) => {
                info!(
                    "copying files into {} before reading them{}, keeping {} free",
                    config.scratch_dir.display(),
                    if reflink {
                        " (reflinks when possible)"
                    } else {
                        ""
                    },
                    format_human_size(config.scratch_min_free)
                );
                Some(scratch)
            }
            Err(error) => {
                warn!(
                    "{} is not usable ({error}): no reflinks, and files over {} that change \
                     while being read are archived once and flagged",
                    config.scratch_dir.display(),
                    format_human_size(config.retry_buffer_size)
                );
                None
            }
        };
        Self {
            compression: Compression::from_config(config),
            scratch,
            reflink,
            retry: RetryPolicy {
                attempts: config.retry_attempts,
                delay: config.retry_delay,
                budget: config.retry_volume_budget,
                buffer_size: config.retry_buffer_size,
                copy_changed_within: config.copy_changed_within,
            },
            #[cfg(test)]
            before_read: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RetryPolicy {
    /// Total reads of a file, including the first one.
    attempts: u32,
    delay: Duration,
    budget: Duration,
    buffer_size: u64,
    /// Large files changed within this time are copied before reading.
    copy_changed_within: Duration,
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

#[derive(Debug, Clone, ValueEnum)]
enum RetentionPolicyKind {
    Count,
    Size,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Back up one volume.
    Backup { volume: String },
    /// Restore one volume, defaulting to the newest archive.
    Restore {
        volume: String,
        #[arg(long)]
        archive: Option<String>,
    },
    /// Restore every subdirectory under `VOLUMES_ROOT`.
    RestoreAll {
        #[arg(long)]
        archive: Option<String>,
    },
    /// Back up every subdirectory under `VOLUMES_ROOT`.
    BackupAll,
    /// Apply retention policy to every discovered volume.
    Cleanup,
    /// Run the cron scheduler.
    #[command(alias = "run")]
    Schedule {
        /// Cron expression for scheduled backup runs.
        #[arg(long = "backup-cron", env = "BACKUP_CRON", default_value = DEFAULT_BACKUP_CRON)]
        backup_cron: String,
    },
}

#[derive(Debug, Clone)]
struct ArchiveInfo {
    name: String,
    size: u64,
}

struct CountingWriter<W> {
    inner: W,
    bytes_written: u64,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            bytes_written: 0,
        }
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.bytes_written = self
            .bytes_written
            .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

trait Storage {
    fn put(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Write + '_>>;
    fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>>;
    /// Lists finished archives and leftover partial archives, sorted by name.
    fn list(&mut self, volume: &str) -> Result<Vec<ArchiveInfo>>;
    fn remove(&mut self, volume: &str, archive: &str) -> Result<()>;
    fn rename(&mut self, volume: &str, from: &str, to: &str) -> Result<()>;
}

struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn volume_dir(&self, volume: &str) -> PathBuf {
        self.root.join(volume)
    }
}

impl Storage for LocalStorage {
    fn put(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Write + '_>> {
        let dir = self.volume_dir(volume);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create storage directory {}", dir.display()))?;
        let path = dir.join(archive);
        let file = File::create(&path)
            .with_context(|| format!("failed to create archive {}", path.display()))?;
        Ok(Box::new(BufWriter::with_capacity(
            STORAGE_BUFFER_SIZE,
            file,
        )))
    }

    fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>> {
        let path = self.volume_dir(volume).join(archive);
        let file = File::open(&path)
            .with_context(|| format!("failed to open archive {}", path.display()))?;
        Ok(Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, file)))
    }

    fn list(&mut self, volume: &str) -> Result<Vec<ArchiveInfo>> {
        let dir = self.volume_dir(volume);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut archives = Vec::new();
        for entry in
            fs::read_dir(&dir).with_context(|| format!("failed to read {}", dir.display()))?
        {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if is_archive_name(&name) || is_partial_name(&name) {
                archives.push(ArchiveInfo {
                    name,
                    size: metadata.len(),
                });
            }
        }
        archives.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(archives)
    }

    fn remove(&mut self, volume: &str, archive: &str) -> Result<()> {
        let path = self.volume_dir(volume).join(archive);
        fs::remove_file(&path)
            .with_context(|| format!("failed to remove archive {}", path.display()))
    }

    fn rename(&mut self, volume: &str, from: &str, to: &str) -> Result<()> {
        let dir = self.volume_dir(volume);
        let (from, to) = (dir.join(from), dir.join(to));
        fs::rename(&from, &to)
            .with_context(|| format!("failed to rename {} to {}", from.display(), to.display()))
    }
}

struct SftpStorage {
    sftp: Sftp,
    root: String,
}

impl SftpStorage {
    fn connect(config: &Cli) -> Result<Self> {
        let url = config
            .sftp_url
            .as_ref()
            .context("SFTP_URL is required when STORAGE_DRIVER=sftp")?;
        let parsed = Url::parse(url).context("failed to parse SFTP_URL")?;
        if parsed.scheme() != "ssh" && parsed.scheme() != "sftp" {
            bail!("SFTP_URL must use ssh:// or sftp://");
        }

        let host = parsed.host_str().context("SFTP_URL must include a host")?;
        let port = parsed.port().unwrap_or(22);
        let username = if parsed.username().is_empty() {
            env::var("USER").unwrap_or_else(|_| "backup".to_owned())
        } else {
            parsed.username().to_owned()
        };
        let root = normalize_remote_dir(parsed.path());

        let tcp = TcpStream::connect((host, port))
            .with_context(|| format!("failed to connect to {host}:{port}"))?;
        let mut session = Session::new().context("failed to create SSH session")?;
        session.set_tcp_stream(tcp);
        session.handshake().context("SSH handshake failed")?;

        let password = secret_value(config.sftp_password.as_ref(), &config.sftp_password_file)?;
        let private_key = secret_value(
            config.sftp_private_key.as_ref(),
            &config.sftp_private_key_file,
        )?;
        let private_key_passphrase = secret_value(
            config.sftp_private_key_passphrase.as_ref(),
            &config.sftp_private_key_passphrase_file,
        )?;

        match (private_key.as_deref(), password.as_deref()) {
            (Some(key), _) => session
                .userauth_pubkey_memory(&username, None, key, private_key_passphrase.as_deref())
                .context("SSH private-key authentication failed")?,
            (None, Some(password)) => session
                .userauth_password(&username, password)
                .context("SSH password authentication failed")?,
            (None, None) => bail!(
                "SFTP authentication requires SFTP_PRIVATE_KEY, SFTP_PRIVATE_KEY_FILE, SFTP_PASSWORD, or SFTP_PASSWORD_FILE"
            ),
        }

        if !session.authenticated() {
            bail!("SSH authentication did not complete");
        }

        info!("connected to SFTP {host}:{port} as {username}, root {root}");
        let sftp = session.sftp().context("failed to open SFTP subsystem")?;
        Ok(Self { sftp, root })
    }

    fn path(&self, volume: &str, archive: Option<&str>) -> String {
        match archive {
            Some(archive) => format!("{}/{volume}/{archive}", self.root),
            None => format!("{}/{volume}", self.root),
        }
    }
}

impl Storage for SftpStorage {
    fn put(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Write + '_>> {
        let dir = self.path(volume, None);
        create_remote_dir_all(&self.sftp, &dir)?;
        let path = self.path(volume, Some(archive));
        let file = self
            .sftp
            .create(Path::new(&path))
            .with_context(|| format!("failed to create remote archive {path}"))?;
        Ok(Box::new(BufWriter::with_capacity(
            STORAGE_BUFFER_SIZE,
            file,
        )))
    }

    fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>> {
        let path = self.path(volume, Some(archive));
        let file = self
            .sftp
            .open(Path::new(&path))
            .with_context(|| format!("failed to open remote archive {path}"))?;
        Ok(Box::new(BufReader::with_capacity(
            STORAGE_BUFFER_SIZE,
            file,
        )))
    }

    fn list(&mut self, volume: &str) -> Result<Vec<ArchiveInfo>> {
        let dir = self.path(volume, None);
        let entries = match self.sftp.readdir(Path::new(&dir)) {
            Ok(entries) => entries,
            Err(error) if is_missing_remote_entry(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error).with_context(|| format!("failed to list {dir}")),
        };

        let mut archives = Vec::new();
        for (path, stat) in entries {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !is_regular_file(&stat) || !(is_archive_name(name) || is_partial_name(name)) {
                continue;
            }
            archives.push(ArchiveInfo {
                name: name.to_owned(),
                size: stat.size.unwrap_or(0),
            });
        }
        archives.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(archives)
    }

    fn remove(&mut self, volume: &str, archive: &str) -> Result<()> {
        let path = self.path(volume, Some(archive));
        self.sftp
            .unlink(Path::new(&path))
            .with_context(|| format!("failed to remove remote archive {path}"))
    }

    fn rename(&mut self, volume: &str, from: &str, to: &str) -> Result<()> {
        let from = self.path(volume, Some(from));
        let to = self.path(volume, Some(to));
        self.sftp
            .rename(Path::new(&from), Path::new(&to), None)
            .with_context(|| format!("failed to rename remote archive {from} to {to}"))
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    if env::args_os().len() == 1 {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    }

    let cli = Cli::parse();
    validate_config(&cli)?;

    match &cli.command {
        Command::Backup { volume } => {
            info!("executing backup command for volume {volume}");
            let mut storage = build_storage(&cli)?;
            backup_volume(
                &cli.volumes_root,
                storage.as_mut(),
                &BackupOptions::from_config(&cli),
                volume,
                None,
            )?;
        }
        Command::Restore { volume, archive } => {
            info!("executing restore command for volume {volume}");
            let mut storage = build_storage(&cli)?;
            restore_volume(
                &cli.volumes_root,
                storage.as_mut(),
                volume,
                archive.as_deref(),
            )?;
        }
        Command::RestoreAll { archive } => {
            info!("executing restore-all command");
            let mut storage = build_storage(&cli)?;
            restore_all(&cli.volumes_root, storage.as_mut(), archive.as_deref())?;
        }
        Command::BackupAll => {
            info!("executing backup-all command");
            let mut storage = build_storage(&cli)?;
            backup_all(
                &cli.volumes_root,
                storage.as_mut(),
                &BackupOptions::from_config(&cli),
            )?
            .into_result("backup")?;
        }
        Command::Cleanup => {
            info!("executing cleanup command");
            let mut storage = build_storage(&cli)?;
            cleanup_all(&cli.volumes_root, storage.as_mut(), &retention_policy(&cli))?;
        }
        Command::Schedule { backup_cron } => {
            info!("executing schedule command");
            run_scheduler(&cli, backup_cron)?;
        }
    }

    Ok(())
}

fn build_storage(config: &Cli) -> Result<Box<dyn Storage>> {
    match config.storage_driver {
        StorageDriverKind::Local => Ok(Box::new(LocalStorage::new(
            config.local_storage_root.clone(),
        ))),
        StorageDriverKind::Sftp => Ok(Box::new(SftpStorage::connect(config)?)),
    }
}

fn validate_config(config: &Cli) -> Result<()> {
    if config.retention_count == 0 {
        bail!("RETENTION_COUNT must be greater than 0");
    }
    if config.retention_min_count == 0 {
        bail!("RETENTION_MIN_COUNT must be greater than 0");
    }
    if config.retry_attempts == 0 {
        bail!("RETRY_ATTEMPTS must be greater than 0");
    }
    if let Some(level) = config.compression_level {
        let valid = match config.compression {
            CompressionKind::Zstd => (1..=22).contains(&level),
            CompressionKind::Xz => level <= 9,
        };
        if !valid {
            bail!(
                "COMPRESSION_LEVEL {level} is out of range for {:?}",
                config.compression
            );
        }
    }
    Ok(())
}

fn backup_volume(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    options: &BackupOptions,
    volume: &str,
    progress: Option<(usize, usize)>,
) -> Result<ArchiveInfo> {
    validate_volume_name(volume)?;
    let source = volumes_root.join(volume);
    if !source.is_dir() {
        bail!("volume {volume} does not exist at {}", source.display());
    }

    let archive = format!(
        "{}{}",
        Utc::now().format("%Y-%m-%dT%H-%M-%SZ"),
        options.compression.kind.extension()
    );
    let partial = format!("{archive}{PARTIAL_SUFFIX}");
    let written = write_archive(storage, options, volume, &partial, &source).and_then(|written| {
        storage.rename(volume, &partial, &archive)?;
        Ok(written)
    });
    let (size, report) = match written {
        Ok(written) => written,
        Err(error) => {
            if let Err(remove_error) = storage.remove(volume, &partial) {
                warn!("failed to remove partial archive {volume}/{partial}: {remove_error:#}");
            }
            return Err(error);
        }
    };
    let archive_info = ArchiveInfo {
        name: archive,
        size,
    };
    log_backup_completed(volume, &source, &archive_info, &report, progress);
    Ok(archive_info)
}

/// Streams `source` into a new archive and returns the compressed size and
/// what happened to the volume's files.
fn write_archive(
    storage: &mut dyn Storage,
    options: &BackupOptions,
    volume: &str,
    name: &str,
    source: &Path,
) -> Result<(u64, VolumeReport)> {
    let compression = options.compression;
    let writer = CountingWriter::new(storage.put(volume, name)?);
    let (mut writer, report) = match compression.kind {
        CompressionKind::Zstd => {
            let level = i32::try_from(compression.level).context("invalid zstd level")?;
            let encoder =
                zstd::Encoder::new(writer, level).context("failed to create zstd encoder")?;
            let mut tar = Builder::new(encoder);
            let report = VolumeArchiver::new(&mut tar, source, volume, options).run()?;
            let encoder = tar.into_inner().context("failed to finish tar stream")?;
            let writer = encoder.finish().context("failed to finish zstd stream")?;
            (writer, report)
        }
        CompressionKind::Xz => {
            let encoder = XzEncoder::new(writer, compression.level);
            let mut tar = Builder::new(encoder);
            let report = VolumeArchiver::new(&mut tar, source, volume, options).run()?;
            let encoder = tar.into_inner().context("failed to finish tar stream")?;
            let writer = encoder.finish().context("failed to finish xz stream")?;
            (writer, report)
        }
    };
    writer.flush().context("failed to flush archive stream")?;
    Ok((writer.bytes_written(), report))
}

fn restore_volume(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    volume: &str,
    archive: Option<&str>,
) -> Result<()> {
    validate_volume_name(volume)?;
    let target = volumes_root.join(volume);
    fs::create_dir_all(&target)
        .with_context(|| format!("failed to create volume directory {}", target.display()))?;

    let archive = match archive {
        Some(archive) => archive.to_owned(),
        None => latest_archive(storage, volume)?.context("no backup archives found")?,
    };
    info!("restoring {volume} from {archive}");
    let kind = CompressionKind::from_archive_name(&archive)
        .with_context(|| format!("unsupported archive format {archive}"))?;
    let reader = storage.get(volume, &archive)?;
    let decoder: Box<dyn Read + '_> = match kind {
        CompressionKind::Zstd => {
            Box::new(zstd::Decoder::new(reader).context("failed to create zstd decoder")?)
        }
        CompressionKind::Xz => Box::new(BufReader::with_capacity(
            READ_BUFFER_SIZE,
            XzDecoder::new(reader),
        )),
    };
    let mut tar = Archive::new(decoder);
    tar.unpack(&target)
        .with_context(|| format!("failed to unpack archive into {}", target.display()))
}

/// Backs up every volume, continuing past volumes that fail. Returns the
/// volumes that were backed up and the ones that failed.
fn backup_all(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    options: &BackupOptions,
) -> Result<VolumeResults> {
    let volumes = discover_volumes(volumes_root)?;
    info!("backing up {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        let progress = (index + 1, volumes.len());
        match backup_volume(volumes_root, storage, options, volume, Some(progress)) {
            Ok(_) => results.succeeded.push(volume.clone()),
            Err(error) => {
                error!(
                    "backup of volume {volume} ({}/{}) failed: {error:#}",
                    progress.0, progress.1
                );
                results.failed.push(volume.clone());
            }
        }
    }
    Ok(results)
}

#[derive(Debug, Default)]
struct VolumeResults {
    succeeded: Vec<String>,
    failed: Vec<String>,
}

impl VolumeResults {
    fn into_result(self, action: &str) -> Result<()> {
        if self.failed.is_empty() {
            return Ok(());
        }
        bail!(
            "{action} failed for {} of {} volume(s): {}",
            self.failed.len(),
            self.failed.len() + self.succeeded.len(),
            self.failed.join(", ")
        )
    }
}

fn restore_all(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    archive: Option<&str>,
) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    info!("restoring {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "restoring volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        match restore_volume(volumes_root, storage, volume, archive) {
            Ok(()) => results.succeeded.push(volume.clone()),
            Err(error) => {
                error!("restore of volume {volume} failed: {error:#}");
                results.failed.push(volume.clone());
            }
        }
    }
    results.into_result("restore")
}

fn cleanup_all(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    policy: &RetentionPolicy,
) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    cleanup_volumes(storage, &volumes, policy)
}

/// Applies retention to each volume, continuing past volumes that fail.
fn cleanup_volumes(
    storage: &mut dyn Storage,
    volumes: &[String],
    policy: &RetentionPolicy,
) -> Result<()> {
    info!("cleaning up {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "cleaning up volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        match cleanup_volume(storage, volume, policy) {
            Ok(()) => results.succeeded.push(volume.clone()),
            Err(error) => {
                error!("cleanup of volume {volume} failed: {error:#}");
                results.failed.push(volume.clone());
            }
        }
    }
    results.into_result("cleanup")
}

fn cleanup_volume(storage: &mut dyn Storage, volume: &str, policy: &RetentionPolicy) -> Result<()> {
    let (archives, partials): (Vec<_>, Vec<_>) = storage
        .list(volume)?
        .into_iter()
        .partition(|archive| is_archive_name(&archive.name));
    // Partial archives are left behind only by interrupted runs. Cleanup runs
    // after backups finish, so nothing is writing them anymore.
    for partial in partials {
        info!(
            "removing leftover partial archive {volume}/{}",
            partial.name
        );
        if let Err(error) = storage.remove(volume, &partial.name) {
            warn!("failed to remove {volume}/{}: {error:#}", partial.name);
        }
    }
    let to_remove = match *policy {
        RetentionPolicy::Count { keep } => archives
            .iter()
            .take(archives.len().saturating_sub(keep))
            .map(|archive| archive.name.clone())
            .collect::<Vec<_>>(),
        RetentionPolicy::Size {
            min_count,
            max_total_size,
        } => archives_to_remove_for_size(&archives, min_count, max_total_size),
    };
    let keep_count = archives.len().saturating_sub(to_remove.len());
    info!(
        "cleanup for volume {volume}: found {} archive(s), removing {}, leaving {}",
        archives.len(),
        to_remove.len(),
        keep_count
    );

    let mut failed = 0;
    for archive in to_remove {
        info!("removing {volume}/{archive}");
        if let Err(error) = storage.remove(volume, &archive) {
            warn!("failed to remove {volume}/{archive}: {error:#}");
            failed += 1;
        }
    }
    if failed > 0 {
        bail!("failed to remove {failed} archive(s)");
    }
    Ok(())
}

fn run_scheduler(config: &Cli, backup_cron: &str) -> Result<()> {
    let schedule = parse_schedule(backup_cron)?;
    info!("cron scheduler started");
    info!("cron expression: {backup_cron}");
    let mut next = next_scheduled_run(&schedule)?;
    info!("next backup run at {next}");

    loop {
        sleep_until_run(next);

        let result = (|| -> Result<()> {
            let mut storage = build_storage(config)?;
            let results = backup_all(
                &config.volumes_root,
                storage.as_mut(),
                &BackupOptions::from_config(config),
            )?;
            // Only rotate archives of volumes that just got a fresh backup, so a
            // failing volume keeps all of its older archives.
            let cleanup = cleanup_volumes(
                storage.as_mut(),
                &results.succeeded,
                &retention_policy(config),
            );
            results.into_result("backup")?;
            cleanup
        })();

        match result {
            Ok(()) => info!("scheduled backup completed"),
            Err(error) => warn!("scheduled backup failed: {error:#}"),
        }

        next = next_scheduled_run(&schedule)?;
        info!("next backup run at {next}");
    }
}

#[derive(Debug)]
enum RetentionPolicy {
    Count {
        keep: usize,
    },
    Size {
        min_count: usize,
        max_total_size: u64,
    },
}

fn retention_policy(config: &Cli) -> RetentionPolicy {
    match config.retention_policy {
        RetentionPolicyKind::Count => RetentionPolicy::Count {
            keep: config.retention_count,
        },
        RetentionPolicyKind::Size => RetentionPolicy::Size {
            min_count: config.retention_min_count,
            max_total_size: config.retention_max_total_size,
        },
    }
}

/// What happened to a volume's files during a backup.
#[derive(Debug, Default)]
struct VolumeReport {
    files: usize,
    /// Files archived from a reflink copy.
    reflinked: usize,
    /// Reads that fully copied a file into the scratch directory.
    copied: usize,
    /// Files that changed while being read and were read again later.
    retried: usize,
    /// Files whose archived copy may be inconsistent.
    unstable: Vec<PathBuf>,
    /// Entries that could not be archived at all.
    skipped: usize,
}

impl VolumeReport {
    fn warnings(&self) -> usize {
        self.skipped + self.unstable.len()
    }
}

enum FileOutcome {
    /// Archived from a reflink copy, or from a read during which the file
    /// didn't change.
    Stable,
    /// Changed while being read. `written` tells whether that read was
    /// archived anyway.
    Changed { written: bool, reason: String },
    /// Not archived: removed, unreadable, or no longer a regular file.
    Skipped(String),
}

/// Appends one volume to the archive. Files that vanish or can't be read are
/// skipped with a warning instead of failing the backup. Small files that
/// change while being read are read again after the walk, and archived only
/// once. Large streamed files can't be taken back out of the archive, so a
/// change is only flagged. Errors are returned only when writing the archive
/// itself fails.
struct VolumeArchiver<'a, W: Write> {
    tar: &'a mut Builder<W>,
    root: &'a Path,
    volume: &'a str,
    retry: RetryPolicy,
    scratch: Option<VolumeScratch<'a>>,
    report: VolumeReport,
    #[cfg(test)]
    before_read: Option<&'a tests::Hook>,
}

impl<'a, W: Write> VolumeArchiver<'a, W> {
    fn new(
        tar: &'a mut Builder<W>,
        root: &'a Path,
        volume: &'a str,
        options: &'a BackupOptions,
    ) -> Self {
        Self {
            tar,
            root,
            volume,
            retry: options.retry,
            scratch: options
                .scratch
                .as_ref()
                .map(|scratch| VolumeScratch::new(scratch, options.reflink)),
            report: VolumeReport::default(),
            #[cfg(test)]
            before_read: options.before_read.as_ref(),
        }
    }

    fn run(mut self) -> Result<VolumeReport> {
        let retries_enabled = self.retry.attempts > 1;
        let mut pending = Vec::new();

        for entry in WalkDir::new(self.root)
            .follow_links(false)
            .sort_by_file_name()
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    let path = error.path().unwrap_or(self.root).to_owned();
                    if is_not_found(error.io_error()) {
                        // Deleted between listing its parent and reading it.
                        warn!("skipping {}: removed during backup", path.display());
                    } else {
                        warn!("skipping {}: cannot read: {error}", path.display());
                    }
                    self.report.skipped += 1;
                    continue;
                }
            };
            let path = entry.path();
            let Ok(relative) = path.strip_prefix(self.root) else {
                continue;
            };
            if relative.as_os_str().is_empty() {
                continue;
            }

            let file_type = entry.file_type();
            if file_type.is_dir() {
                let metadata = match fs::symlink_metadata(path) {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        self.skip(path, &describe_source_error(&error));
                        continue;
                    }
                };
                let mut header = Header::new_gnu();
                header.set_metadata_in_mode(&metadata, HeaderMode::Complete);
                header.set_entry_type(EntryType::Directory);
                header.set_size(0);
                self.tar.append_data(&mut header, relative, io::empty())?;
            } else if file_type.is_file() {
                self.report.files += 1;
                match self.archive_file(path, relative, !retries_enabled)? {
                    FileOutcome::Stable => {}
                    FileOutcome::Changed { written, reason } if written || !retries_enabled => {
                        self.unstable(relative, &reason);
                    }
                    FileOutcome::Changed { reason, .. } => {
                        info!("{}: {reason}, will read it again", path.display());
                        pending.push(relative.to_owned());
                    }
                    FileOutcome::Skipped(reason) => self.skip(path, &reason),
                }
            } else if file_type.is_symlink() {
                let target = match fs::read_link(path) {
                    Ok(target) => target,
                    Err(error) => {
                        self.skip(path, &describe_source_error(&error));
                        continue;
                    }
                };
                let mut header = Header::new_gnu();
                header.set_entry_type(EntryType::Symlink);
                header.set_size(0);
                header.set_mode(0o777);
                if let Some(modified) = entry
                    .metadata()
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())
                {
                    set_header_mtime(&mut header, modified);
                }
                header.set_cksum();
                self.tar.append_link(&mut header, relative, target)?;
            }
        }

        self.retry_changed(pending)?;
        if let Some(scratch) = &self.scratch {
            self.report.reflinked = scratch.reflinked;
            self.report.copied = scratch.copied;
        }
        Ok(self.report)
    }

    /// Reads changed files again, in passes, until each one is read without
    /// changing or runs out of attempts, or the volume's retry budget is
    /// used up. Nothing is archived until a read is stable or the last
    /// attempt, except a file that has grown too big to buffer, which is
    /// streamed once.
    fn retry_changed(&mut self, mut pending: Vec<PathBuf>) -> Result<()> {
        self.report.retried = pending.len();
        let started = Instant::now();
        let mut attempt = 1;
        while !pending.is_empty() {
            attempt += 1;
            let mut out_of_budget = started.elapsed() + self.retry.delay >= self.retry.budget;
            if !out_of_budget {
                thread::sleep(self.retry.delay);
            }
            info!(
                "volume {}: reading {} changed file(s) again, attempt {attempt} of {}",
                self.volume,
                pending.len(),
                self.retry.attempts
            );
            for relative in std::mem::take(&mut pending) {
                out_of_budget |= started.elapsed() >= self.retry.budget;
                let last = attempt >= self.retry.attempts || out_of_budget;
                let path = self.root.join(&relative);
                match self.archive_file(&path, &relative, last)? {
                    FileOutcome::Stable => {
                        info!(
                            "{}: archived unchanged on attempt {attempt}",
                            path.display()
                        );
                    }
                    FileOutcome::Changed { reason, .. } if last => {
                        let limit = if out_of_budget {
                            "retry budget of the volume used up"
                        } else {
                            "out of attempts"
                        };
                        self.unstable(&relative, &format!("{reason}, {limit}"));
                    }
                    FileOutcome::Changed {
                        written: true,
                        reason,
                    } => {
                        self.unstable(&relative, &reason);
                    }
                    FileOutcome::Changed { .. } => pending.push(relative),
                    FileOutcome::Skipped(reason) => self.skip(&path, &reason),
                }
            }
        }
        Ok(())
    }

    /// Archives one regular file: from a reflink copy when possible, else
    /// from memory for small files or a full copy for large ones (either
    /// archived only once a read was stable or on the `last` attempt), else
    /// streamed from the live file.
    fn archive_file(&mut self, path: &Path, relative: &Path, last: bool) -> Result<FileOutcome> {
        // Take the metadata from the open handle, so the header describes the
        // exact file being read even if the path is replaced meanwhile.
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) => return Ok(FileOutcome::Skipped(describe_source_error(&error))),
        };
        let before = match file.metadata() {
            Ok(metadata) => metadata,
            Err(error) => return Ok(FileOutcome::Skipped(describe_source_error(&error))),
        };
        if !before.is_file() {
            return Ok(FileOutcome::Skipped("no longer a regular file".to_owned()));
        }

        if before.len() > 0
            && let Some(scratch) = &mut self.scratch
            && let Some(copy) = scratch.reflink(&file, before.len(), path, self.volume)
        {
            // The copy can't change. Its size and the source's metadata right
            // after the copy describe the archived content.
            self.before_read(path);
            let (Ok(source), Ok(copied)) = (file.metadata(), copy.metadata()) else {
                return Ok(FileOutcome::Skipped("cannot stat reflink copy".to_owned()));
            };
            let mut header = Header::new_gnu();
            header.set_metadata_in_mode(&source, HeaderMode::Complete);
            header.set_size(copied.len());
            return self.append_streamed(&copy, header, &copied, relative);
        }

        self.before_read(path);
        if self.retry.attempts > 1 {
            if before.len() <= self.retry.buffer_size {
                return self.append_buffered(&file, &before, relative, last);
            }
            if changed_within(&before, self.retry.copy_changed_within)
                && let Some(scratch) = &mut self.scratch
                && let Some(copy) = scratch.copy(&file, before.len(), path, self.volume)
            {
                return self.append_copied(&file, &copy, &before, relative, last);
            }
        }

        let mut header = Header::new_gnu();
        header.set_metadata_in_mode(&before, HeaderMode::Complete);
        self.append_streamed(&file, header, &before, relative)
    }

    /// Streams `file` into the archive; `before` is its metadata from before
    /// the read, to detect changes.
    fn append_streamed(
        &mut self,
        file: &File,
        mut header: Header,
        before: &fs::Metadata,
        relative: &Path,
    ) -> Result<FileOutcome> {
        let mut reader = FixedSizeReader::new(file, header.size()?);
        // The outer BufReader makes io::copy move data in large blocks.
        self.tar.append_data(
            &mut header,
            relative,
            BufReader::with_capacity(READ_BUFFER_SIZE, &mut reader),
        )?;
        Ok(match reader.problem(before) {
            None => FileOutcome::Stable,
            Some(reason) => FileOutcome::Changed {
                written: true,
                reason,
            },
        })
    }

    /// Reads a small file into memory and archives it only if it didn't
    /// change meanwhile, or if this is the `last` attempt.
    fn append_buffered(
        &mut self,
        file: &File,
        before: &fs::Metadata,
        relative: &Path,
        last: bool,
    ) -> Result<FileOutcome> {
        let mut data = Vec::with_capacity(usize::try_from(before.len()).unwrap_or_default());
        if let Err(error) = file.take(before.len()).read_to_end(&mut data) {
            let reason = format!("read failed ({error})");
            return Ok(if last {
                FileOutcome::Skipped(reason)
            } else {
                FileOutcome::Changed {
                    written: false,
                    reason,
                }
            });
        }
        let stable = data.len() as u64 == before.len()
            && file
                .metadata()
                .is_ok_and(|after| same_version(before, &after));
        if stable || last {
            let mut header = Header::new_gnu();
            header.set_metadata_in_mode(before, HeaderMode::Complete);
            header.set_size(data.len() as u64);
            self.tar.append_data(&mut header, relative, &data[..])?;
        }
        Ok(if stable {
            FileOutcome::Stable
        } else {
            FileOutcome::Changed {
                written: last,
                reason: "changed while being read".to_owned(),
            }
        })
    }

    /// Archives `copy`, a full copy of `file`, only if `file` didn't change
    /// while it was copied, or if this is the `last` attempt.
    fn append_copied(
        &mut self,
        file: &File,
        copy: &File,
        before: &fs::Metadata,
        relative: &Path,
        last: bool,
    ) -> Result<FileOutcome> {
        let Ok(copied) = copy.metadata() else {
            return Ok(FileOutcome::Skipped("cannot stat copy".to_owned()));
        };
        let stable = copied.len() == before.len()
            && file
                .metadata()
                .is_ok_and(|after| same_version(before, &after));
        if stable || last {
            let mut header = Header::new_gnu();
            header.set_metadata_in_mode(before, HeaderMode::Complete);
            header.set_size(copied.len());
            self.append_streamed(copy, header, &copied, relative)?;
        }
        Ok(if stable {
            FileOutcome::Stable
        } else {
            FileOutcome::Changed {
                written: last,
                reason: "changed while being copied".to_owned(),
            }
        })
    }

    #[cfg_attr(not(test), allow(clippy::unused_self, unused_variables))]
    fn before_read(&self, path: &Path) {
        #[cfg(test)]
        if let Some(hook) = self.before_read {
            (hook.0)(path);
        }
    }

    fn skip(&mut self, path: &Path, reason: &str) {
        warn!("skipping {}: {reason}", path.display());
        self.report.skipped += 1;
    }

    fn unstable(&mut self, relative: &Path, reason: &str) {
        warn!(
            "{}: {reason}, archived copy may be inconsistent",
            self.root.join(relative).display()
        );
        self.report.unstable.push(relative.to_owned());
    }
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
/// or entirely after it. Elsewhere, files too large to read into memory are
/// fully copied, so a copy can be checked before it's archived.
#[derive(Debug)]
struct Scratch {
    dir: PathBuf,
    min_free: u64,
}

impl Scratch {
    fn new(dir: &Path, min_free: u64) -> io::Result<Self> {
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
struct VolumeScratch<'a> {
    scratch: &'a Scratch,
    reflink_enabled: bool,
    reflinked: usize,
    copied: usize,
    low_space_logged: bool,
}

impl<'a> VolumeScratch<'a> {
    fn new(scratch: &'a Scratch, reflink: bool) -> Self {
        Self {
            scratch,
            reflink_enabled: reflink,
            reflinked: 0,
            copied: 0,
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
                self.reflinked += 1;
                Some(copy)
            }
            Ok(Err(free)) => {
                self.log_low_space(free, path, volume);
                None
            }
            Err(error) if self.reflinked == 0 && is_reflink_unsupported(&error) => {
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

    /// Returns a full copy of the first `size` bytes of `file`, positioned
    /// at its start, or `None` when it can't be made.
    fn copy(&mut self, file: &File, size: u64, path: &Path, volume: &str) -> Option<File> {
        let result = self
            .scratch
            .temp_file_with_room(size)
            .and_then(|copy| match copy {
                Ok(mut copy) => {
                    // Uses copy_file_range, so the data doesn't pass through
                    // this process.
                    io::copy(&mut file.take(size), &mut copy)?;
                    copy.rewind()?;
                    Ok(Ok(copy))
                }
                Err(free) => Ok(Err(free)),
            });
        match result {
            Ok(Ok(copy)) => {
                self.copied += 1;
                Some(copy)
            }
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

fn is_not_found(error: Option<&io::Error>) -> bool {
    error.is_some_and(|error| error.kind() == io::ErrorKind::NotFound)
}

/// Yields exactly `size` bytes so a tar entry always matches its header: a
/// file that grew is cut at `size`, and a file that shrank or failed to read
/// part-way is padded with zeros.
struct FixedSizeReader<R> {
    inner: R,
    remaining: u64,
    padded: u64,
    error: Option<io::Error>,
    eof: bool,
}

impl<R> FixedSizeReader<R> {
    fn new(inner: R, size: u64) -> Self {
        Self {
            inner,
            remaining: size,
            padded: 0,
            error: None,
            eof: false,
        }
    }
}

impl FixedSizeReader<&File> {
    /// Describes anything that went wrong while the file was archived.
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

fn discover_volumes(root: &Path) -> Result<Vec<String>> {
    let mut volumes = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry = match entry.and_then(|entry| entry.metadata().map(|meta| (entry, meta))) {
            Ok((entry, metadata)) if metadata.is_dir() => entry,
            Ok(_) => continue,
            Err(error) => {
                warn!("skipping unreadable entry in {}: {error}", root.display());
                continue;
            }
        };
        let name = entry.file_name().to_string_lossy().into_owned();
        validate_volume_name(&name)?;
        volumes.push(name);
    }
    volumes.sort();
    Ok(volumes)
}

fn latest_archive(storage: &mut dyn Storage, volume: &str) -> Result<Option<String>> {
    Ok(storage
        .list(volume)?
        .into_iter()
        .rev()
        .find(|archive| is_archive_name(&archive.name))
        .map(|archive| archive.name))
}

fn archives_to_remove_for_size(
    archives: &[ArchiveInfo],
    min_count: usize,
    max_total_size: u64,
) -> Vec<String> {
    let mut total = archives.iter().map(|archive| archive.size).sum::<u64>();
    let mut remaining = archives.len();
    let mut to_remove = Vec::new();

    for archive in archives {
        if remaining <= min_count || total <= max_total_size {
            break;
        }
        total = total.saturating_sub(archive.size);
        remaining -= 1;
        to_remove.push(archive.name.clone());
    }

    to_remove
}

fn log_backup_completed(
    volume: &str,
    source: &Path,
    archive: &ArchiveInfo,
    report: &VolumeReport,
    progress: Option<(usize, usize)>,
) {
    let mut details = vec![
        format_human_size(archive.size),
        format!("{} file(s)", report.files),
    ];
    if report.reflinked > 0 {
        details.push(format!("{} reflinked", report.reflinked));
    }
    if report.copied > 0 {
        details.push(format!("{} copied before reading", report.copied));
    }
    if report.retried > 0 {
        details.push(format!("{} read again after changing", report.retried));
    }
    if report.warnings() > 0 {
        details.push(format!(
            "{} possibly inconsistent, {} skipped, see warnings above",
            report.unstable.len(),
            report.skipped
        ));
    }
    let details = details.join(", ");
    match progress {
        Some((index, total)) => info!(
            "backed up volume {volume} ({index}/{total}) from {} to archive {} ({details})",
            source.display(),
            archive.name
        ),
        None => info!(
            "backed up volume {volume} from {} to archive {} ({details})",
            source.display(),
            archive.name
        ),
    }
}

fn format_human_size(bytes: u64) -> String {
    const UNITS: [(&str, u128); 5] = [
        ("b", 1),
        ("kb", 1_000),
        ("mb", 1_000_000),
        ("gb", 1_000_000_000),
        ("tb", 1_000_000_000_000),
    ];

    let bytes = u128::from(bytes);
    let unit_index = UNITS
        .iter()
        .rposition(|(_, divisor)| bytes >= *divisor)
        .unwrap_or(0);
    let (unit, divisor) = UNITS[unit_index];

    if unit_index == 0 {
        return format!("{bytes}{unit}");
    }

    let rounded_tenths = ((bytes * 10) + (divisor / 2)) / divisor;
    if rounded_tenths >= 100 {
        return format!("{}{}", (bytes + (divisor / 2)) / divisor, unit);
    }
    if rounded_tenths % 10 == 0 {
        format!("{}{}", rounded_tenths / 10, unit)
    } else {
        format!("{}.{}{}", rounded_tenths / 10, rounded_tenths % 10, unit)
    }
}

fn parse_schedule(expression: &str) -> Result<Schedule> {
    let fields = expression.split_whitespace().count();
    let cron_expression = match fields {
        5 => format!("0 {expression}"),
        6 | 7 => expression.to_owned(),
        _ => bail!("cron expression must have 5, 6, or 7 fields"),
    };
    Schedule::from_str(&cron_expression)
        .with_context(|| format!("failed to parse cron expression {expression:?}"))
}

fn next_scheduled_run(schedule: &Schedule) -> Result<DateTime<Local>> {
    let now = Local::now();
    schedule
        .after(&now)
        .next()
        .context("cron expression produced no future run")
}

fn sleep_until_run(next: DateTime<Local>) {
    let sleep_for = next
        .signed_duration_since(Local::now())
        .to_std()
        .unwrap_or_default();
    sleep_until(sleep_for);
}

fn sleep_until(duration: Duration) {
    thread::sleep(duration);
}

fn validate_volume_name(volume: &str) -> Result<()> {
    if volume.is_empty()
        || volume == "."
        || volume == ".."
        || volume.contains('/')
        || volume.contains('\\')
    {
        bail!("invalid volume name {volume:?}");
    }
    Ok(())
}

fn is_partial_name(name: &str) -> bool {
    name.strip_suffix(PARTIAL_SUFFIX)
        .is_some_and(is_archive_name)
}

fn is_archive_name(name: &str) -> bool {
    CompressionKind::from_archive_name(name).is_some()
}

fn parse_size(value: &str) -> Result<u64, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("size cannot be empty".to_owned());
    }

    let number_len = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, unit) = value.split_at(number_len);
    let number = number
        .parse::<u64>()
        .map_err(|error| format!("invalid size number: {error}"))?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1_000,
        "m" | "mb" => 1_000_000,
        "g" | "gb" => 1_000_000_000,
        "t" | "tb" => 1_000_000_000_000,
        "ki" | "kib" => 1024,
        "mi" | "mib" => 1024_u64.pow(2),
        "gi" | "gib" => 1024_u64.pow(3),
        "ti" | "tib" => 1024_u64.pow(4),
        other => return Err(format!("unsupported size unit {other:?}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "size is too large".to_owned())
}

fn normalize_remote_dir(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn create_remote_dir_all(sftp: &Sftp, path: &str) -> Result<()> {
    let mut current = String::new();
    for part in path.split('/').filter(|part| !part.is_empty()) {
        current.push('/');
        current.push_str(part);

        match sftp.stat(Path::new(&current)) {
            Ok(stat) if stat.file_type().is_dir() => {}
            Ok(_) => bail!("{current} exists but is not a directory"),
            Err(error) if is_missing_remote_entry(&error) => {
                info!("creating remote directory {current}");
                if let Err(error) = sftp.mkdir(Path::new(&current), 0o755) {
                    match sftp.stat(Path::new(&current)) {
                        Ok(stat) if stat.file_type().is_dir() => {}
                        _ => {
                            return Err(error)
                                .with_context(|| format!("failed to create {current}"));
                        }
                    }
                }
            }
            Err(error) => return Err(error).with_context(|| format!("failed to stat {current}")),
        }
    }
    Ok(())
}

fn is_missing_remote_entry(error: &ssh2::Error) -> bool {
    matches!(
        error.code(),
        ssh2::ErrorCode::Session(-16) | ssh2::ErrorCode::SFTP(2 | 10)
    )
}

fn secret_value(inline: Option<&String>, file: &Path) -> Result<Option<String>> {
    if let Some(value) = inline {
        return Ok(Some(value.clone()));
    }
    match fs::read_to_string(file) {
        Ok(value) => Ok(Some(value.trim_end_matches(['\r', '\n']).to_owned())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read secret {}", file.display()))
        }
    }
}

fn is_regular_file(stat: &FileStat) -> bool {
    stat.file_type().is_file()
}

fn set_header_mtime(header: &mut Header, modified: std::time::SystemTime) {
    let file_time = FileTime::from_system_time(modified);
    let seconds = u64::try_from(file_time.unix_seconds()).unwrap_or_default();
    header.set_mtime(seconds);
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn size_retention_keeps_minimum_count() {
        let archives = vec![
            ArchiveInfo {
                name: "1.tar.xz".to_owned(),
                size: 10,
            },
            ArchiveInfo {
                name: "2.tar.xz".to_owned(),
                size: 10,
            },
            ArchiveInfo {
                name: "3.tar.xz".to_owned(),
                size: 10,
            },
        ];
        assert_eq!(
            archives_to_remove_for_size(&archives, 2, 15),
            vec!["1.tar.xz".to_owned()]
        );
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
    fn restore_all_restores_each_discovered_volume() {
        let temp = tempfile::tempdir().unwrap();
        let volumes_root = temp.path().join("volumes");
        let backups_root = temp.path().join("backups");
        let app_volume = volumes_root.join("app");
        let media_volume = volumes_root.join("media");

        fs::create_dir_all(&app_volume).unwrap();
        fs::create_dir_all(&media_volume).unwrap();
        fs::write(app_volume.join("state.txt"), "old app").unwrap();
        fs::write(media_volume.join("state.txt"), "old media").unwrap();

        let mut storage = LocalStorage::new(backups_root);
        backup_all(&volumes_root, &mut storage, &BackupOptions::default()).unwrap();

        fs::write(app_volume.join("state.txt"), "new app").unwrap();
        fs::write(media_volume.join("state.txt"), "new media").unwrap();

        restore_all(&volumes_root, &mut storage, None).unwrap();

        assert_eq!(
            fs::read_to_string(app_volume.join("state.txt")).unwrap(),
            "old app"
        );
        assert_eq!(
            fs::read_to_string(media_volume.join("state.txt")).unwrap(),
            "old media"
        );
    }

    #[test]
    fn backup_creates_nested_local_storage_directories() {
        let temp = tempfile::tempdir().unwrap();
        let volumes_root = temp.path().join("volumes");
        let backups_root = temp.path().join("missing").join("nested").join("backups");
        let app_volume = volumes_root.join("app");

        fs::create_dir_all(&app_volume).unwrap();
        fs::write(app_volume.join("state.txt"), "app data").unwrap();

        let mut storage = LocalStorage::new(backups_root.clone());
        let archive = backup_volume(
            &volumes_root,
            &mut storage,
            &BackupOptions::default(),
            "app",
            None,
        )
        .unwrap();

        assert!(archive.size > 0);
        assert_eq!(storage.list("app").unwrap().len(), 1);
        assert!(backups_root.join("app").is_dir());
    }

    #[test]
    fn restores_legacy_xz_and_zstd_archives() {
        let temp = tempfile::tempdir().unwrap();
        let volumes_root = temp.path().join("volumes");
        let app_volume = volumes_root.join("app");
        fs::create_dir_all(app_volume.join("nested")).unwrap();
        let payload = (0..300_000u32)
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        fs::write(app_volume.join("nested").join("big.bin"), &payload).unwrap();

        for kind in CompressionKind::ALL {
            let mut storage = LocalStorage::new(temp.path().join(format!("backups-{kind:?}")));
            let options = BackupOptions {
                compression: Compression { kind, level: 1 },
                ..BackupOptions::default()
            };
            let archive =
                backup_volume(&volumes_root, &mut storage, &options, "app", None).unwrap();
            assert!(archive.name.ends_with(kind.extension()));

            fs::remove_dir_all(&app_volume).unwrap();
            restore_volume(&volumes_root, &mut storage, "app", None).unwrap();
            assert_eq!(
                fs::read(app_volume.join("nested").join("big.bin")).unwrap(),
                payload
            );
        }
    }

    #[test]
    fn mixed_archive_formats_sort_by_timestamp() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("app");
        fs::create_dir_all(&dir).unwrap();
        for name in [
            "2026-01-02T01-00-00Z.tar.zst",
            "2026-01-01T01-00-00Z.tar.xz",
            "notes.txt",
        ] {
            fs::write(dir.join(name), "x").unwrap();
        }
        let mut storage = LocalStorage::new(temp.path().to_owned());
        let names = storage
            .list("app")
            .unwrap()
            .into_iter()
            .map(|archive| archive.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "2026-01-01T01-00-00Z.tar.xz",
                "2026-01-02T01-00-00Z.tar.zst"
            ]
        );
    }

    /// Local storage that fails writes for chosen volumes after some bytes.
    struct FlakyStorage {
        inner: LocalStorage,
        fail_volume: &'static str,
    }

    struct FailingWriter<W> {
        inner: W,
        budget: usize,
    }

    impl<W: Write> Write for FailingWriter<W> {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if self.budget == 0 {
                return Err(io::Error::other("storage went away"));
            }
            let len = buffer.len().min(self.budget);
            self.budget -= len;
            self.inner.write(&buffer[..len])
        }

        fn flush(&mut self) -> io::Result<()> {
            self.inner.flush()
        }
    }

    impl Storage for FlakyStorage {
        fn put(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Write + '_>> {
            let fail = volume == self.fail_volume;
            let writer = self.inner.put(volume, archive)?;
            if fail {
                Ok(Box::new(FailingWriter {
                    inner: writer,
                    budget: 100,
                }))
            } else {
                Ok(writer)
            }
        }
        fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>> {
            self.inner.get(volume, archive)
        }
        fn list(&mut self, volume: &str) -> Result<Vec<ArchiveInfo>> {
            self.inner.list(volume)
        }
        fn remove(&mut self, volume: &str, archive: &str) -> Result<()> {
            self.inner.remove(volume, archive)
        }
        fn rename(&mut self, volume: &str, from: &str, to: &str) -> Result<()> {
            self.inner.rename(volume, from, to)
        }
    }

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
    fn shrunk_file_keeps_archive_readable() {
        let mut tar = Builder::new(Vec::new());
        let mut header = Header::new_gnu();
        header.set_size(10);
        header.set_mode(0o644);
        tar.append_data(
            &mut header,
            "short.bin",
            FixedSizeReader::new(&b"1234"[..], 10),
        )
        .unwrap();
        let mut header = Header::new_gnu();
        header.set_size(2);
        header.set_mode(0o644);
        tar.append_data(&mut header, "next.txt", &b"ok"[..])
            .unwrap();
        let bytes = tar.into_inner().unwrap();

        let mut archive = Archive::new(&bytes[..]);
        let mut entries = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            entries.push((entry.path().unwrap().display().to_string(), data));
        }
        assert_eq!(
            entries,
            [
                ("short.bin".to_owned(), b"1234\0\0\0\0\0\0".to_vec()),
                ("next.txt".to_owned(), b"ok".to_vec()),
            ]
        );
    }

    #[test]
    fn unreadable_file_is_skipped_with_warning() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let volumes_root = temp.path().join("volumes");
        let app = volumes_root.join("app");
        fs::create_dir_all(&app).unwrap();
        fs::write(app.join("good.txt"), "good").unwrap();
        fs::write(app.join("secret.txt"), "secret").unwrap();
        fs::set_permissions(app.join("secret.txt"), fs::Permissions::from_mode(0o000)).unwrap();
        if File::open(app.join("secret.txt")).is_ok() {
            // Running as root: permissions cannot make the file unreadable.
            return;
        }

        let mut storage = LocalStorage::new(temp.path().join("backups"));
        backup_volume(
            &volumes_root,
            &mut storage,
            &BackupOptions::default(),
            "app",
            None,
        )
        .unwrap();

        fs::remove_dir_all(&app).unwrap();
        restore_volume(&volumes_root, &mut storage, "app", None).unwrap();
        assert_eq!(fs::read_to_string(app.join("good.txt")).unwrap(), "good");
        assert!(!app.join("secret.txt").exists());
    }

    #[test]
    fn failing_volume_does_not_stop_other_volumes() {
        let temp = tempfile::tempdir().unwrap();
        let volumes_root = temp.path().join("volumes");
        for volume in ["a", "bad", "c"] {
            fs::create_dir_all(volumes_root.join(volume)).unwrap();
            let payload = (0..100_000u32)
                .flat_map(u32::to_le_bytes)
                .collect::<Vec<_>>();
            fs::write(volumes_root.join(volume).join("data.bin"), payload).unwrap();
        }

        let mut storage = FlakyStorage {
            inner: LocalStorage::new(temp.path().join("backups")),
            fail_volume: "bad",
        };
        let results = backup_all(&volumes_root, &mut storage, &BackupOptions::default()).unwrap();
        assert_eq!(results.succeeded, ["a", "c"]);
        assert_eq!(results.failed, ["bad"]);
        // The interrupted archive was cleaned up instead of looking like a backup.
        assert!(storage.list("bad").unwrap().is_empty());
        assert_eq!(storage.list("a").unwrap().len(), 1);
        assert!(results.into_result("backup").is_err());
    }

    #[test]
    fn leftover_partial_archives_are_ignored_and_cleaned_up() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("app");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("2026-01-01T01-00-00Z.tar.zst"), "x").unwrap();
        fs::write(dir.join("2026-01-02T01-00-00Z.tar.zst.partial"), "x").unwrap();

        let mut storage = LocalStorage::new(temp.path().to_owned());
        assert_eq!(
            latest_archive(&mut storage, "app").unwrap().as_deref(),
            Some("2026-01-01T01-00-00Z.tar.zst")
        );
        cleanup_volume(&mut storage, "app", &RetentionPolicy::Count { keep: 1 }).unwrap();
        let names = storage
            .list("app")
            .unwrap()
            .into_iter()
            .map(|archive| archive.name)
            .collect::<Vec<_>>();
        assert_eq!(names, ["2026-01-01T01-00-00Z.tar.zst"]);
    }

    #[derive(Clone)]
    pub(super) struct Hook(pub(super) std::sync::Arc<dyn Fn(&Path) + Send + Sync>);

    impl std::fmt::Debug for Hook {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("Hook")
        }
    }

    const ARCHIVE: &str = "2026-01-01T00-00-00Z.tar.zst";

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
    ) -> (Option<Hook>, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = std::sync::Arc::new(AtomicUsize::new(0));
        let counter = reads.clone();
        let hook = Hook(std::sync::Arc::new(move |path: &Path| {
            if path.file_name().is_some_and(|file| file == name) {
                let read = counter.fetch_add(1, Ordering::SeqCst);
                if read < times {
                    change(path);
                }
            }
        }));
        (Some(hook), reads)
    }

    fn reads(counter: &std::sync::atomic::AtomicUsize) -> usize {
        counter.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Backs up `<root>/volumes/app` into `<root>/backups` and returns the
    /// report and every archive entry in order, duplicates included.
    fn backup_app(root: &Path, options: &BackupOptions) -> (VolumeReport, Vec<(String, Vec<u8>)>) {
        let mut storage = LocalStorage::new(root.join("backups"));
        let (_, report) = write_archive(
            &mut storage,
            options,
            "app",
            ARCHIVE,
            &root.join("volumes").join("app"),
        )
        .unwrap();
        let reader = storage.get("app", ARCHIVE).unwrap();
        let mut archive = Archive::new(zstd::Decoder::new(reader).unwrap());
        let mut entries = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            entries.push((entry.path().unwrap().display().to_string(), data));
        }
        (report, entries)
    }

    fn copies<'a>(entries: &'a [(String, Vec<u8>)], name: &str) -> Vec<&'a [u8]> {
        entries
            .iter()
            .filter(|(path, _)| path == name)
            .map(|(_, data)| data.as_slice())
            .collect()
    }

    fn app_volume(root: &Path, files: &[(&str, &[u8])]) -> PathBuf {
        let app = root.join("volumes").join("app");
        fs::create_dir_all(&app).unwrap();
        for (name, data) in files {
            fs::write(app.join(name), data).unwrap();
        }
        app
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
        let options = BackupOptions {
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        // Bytes already streamed into the archive can't be replaced, so a
        // second read would only add another full copy.
        assert_eq!(reads(&read_count), 1);
        assert_eq!((report.files, report.retried), (2, 0));
        assert_eq!(report.unstable, [PathBuf::from("hot.bin")]);
        assert_eq!(copies(&entries, "hot.bin").len(), 1);
        assert_eq!(copies(&entries, "cold.txt"), [b"cold"]);
    }

    /// Options that fully copy files over 1 KiB into a scratch directory
    /// under `root`, without reflinks.
    fn copying(root: &Path, attempts: u32, before_read: Option<Hook>) -> BackupOptions {
        let dir = root.join("scratch");
        fs::create_dir(&dir).unwrap();
        BackupOptions {
            scratch: Some(Scratch::new(&dir, 0).unwrap()),
            retry: fast_retry(attempts),
            before_read,
            ..BackupOptions::default()
        }
    }

    #[test]
    fn large_file_that_changed_while_copied_is_copied_again_and_archived_once() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("hot.bin", &[b'a'; 4096])]);
        let (before_read, read_count) = change_during_read("hot.bin", 1, |path| {
            fs::write(path, [b'b'; 4096]).unwrap();
        });
        let options = copying(temp.path(), 3, before_read);

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 2);
        assert_eq!((report.retried, report.copied), (1, 2));
        assert_eq!(report.unstable, Vec::<PathBuf>::new());
        assert_eq!(copies(&entries, "hot.bin"), [&[b'b'; 4096][..]]);
        // Copies are unnamed and gone once read.
        assert_eq!(
            fs::read_dir(temp.path().join("scratch")).unwrap().count(),
            0
        );
    }

    #[test]
    fn large_file_that_keeps_changing_is_archived_once_after_the_last_copy() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("hot.bin", &[b'a'; 4096])]);
        let (before_read, read_count) = change_during_read("hot.bin", usize::MAX, |path| {
            let mut file = OpenOptions::new().append(true).open(path).unwrap();
            file.write_all(b"x").unwrap();
        });
        let options = copying(temp.path(), 3, before_read);

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 3);
        assert_eq!(report.unstable, [PathBuf::from("hot.bin")]);
        assert_eq!(copies(&entries, "hot.bin").len(), 1);
    }

    #[test]
    fn large_file_not_changed_recently_is_streamed_without_copying() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("old.bin", &[b'a'; 4096])]);
        let mut options = copying(temp.path(), 3, None);
        options.retry.copy_changed_within = Duration::ZERO;

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(report.copied, 0);
        assert_eq!(copies(&entries, "old.bin"), [&[b'a'; 4096][..]]);
    }

    #[test]
    fn file_that_grows_too_big_to_buffer_is_streamed_once_on_retry() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("db", b"small")]);
        let (before_read, read_count) = change_during_read("db", 1, |path| {
            fs::write(path, [b'b'; 4096]).unwrap();
        });
        let options = BackupOptions {
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 2);
        assert_eq!(report.unstable, Vec::<PathBuf>::new());
        assert_eq!(copies(&entries, "db"), [&[b'b'; 4096][..]]);
    }

    #[test]
    fn small_file_is_archived_once_after_a_stable_read() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("db.sqlite", b"version 1")]);
        let (before_read, read_count) = change_during_read("db.sqlite", 1, |path| {
            fs::write(path, b"version 2").unwrap();
        });
        let options = BackupOptions {
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 2);
        assert_eq!(report.retried, 1);
        assert_eq!(report.unstable, Vec::<PathBuf>::new());
        assert_eq!(copies(&entries, "db.sqlite"), [b"version 2"]);
    }

    #[test]
    fn file_that_keeps_changing_is_archived_after_the_last_attempt() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("log.txt", b"x")]);
        let (before_read, read_count) = change_during_read("log.txt", usize::MAX, |path| {
            let mut file = OpenOptions::new().append(true).open(path).unwrap();
            file.write_all(b"x").unwrap();
        });
        let options = BackupOptions {
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 3);
        assert_eq!(report.unstable, [PathBuf::from("log.txt")]);
        assert_eq!(report.warnings(), 1);
        // Only the last read is archived, as the buffered file never was stable.
        assert_eq!(copies(&entries, "log.txt").len(), 1);
    }

    #[test]
    fn retry_budget_limits_attempts() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("log.txt", b"x")]);
        let (before_read, read_count) = change_during_read("log.txt", usize::MAX, |path| {
            fs::write(path, b"changed").unwrap();
        });
        let options = BackupOptions {
            retry: RetryPolicy {
                budget: Duration::ZERO,
                ..fast_retry(10)
            },
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 2);
        assert_eq!(report.unstable.len(), 1);
        assert_eq!(copies(&entries, "log.txt").len(), 1);
    }

    #[test]
    fn changed_file_removed_before_retry_is_left_out() {
        let temp = tempfile::tempdir().unwrap();
        app_volume(temp.path(), &[("db-journal", b"journal"), ("db", b"db")]);
        let (before_read, _) = change_during_read("db-journal", 1, |path| {
            fs::write(path, b"half-written").unwrap();
            fs::remove_file(path).unwrap();
        });
        let options = BackupOptions {
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(copies(&entries, "db-journal"), Vec::<&[u8]>::new());
        assert_eq!(copies(&entries, "db"), [b"db"]);
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
        let options = BackupOptions {
            retry: fast_retry(1),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(reads(&read_count), 1);
        assert_eq!(report.retried, 0);
        assert_eq!(report.unstable, [PathBuf::from("db")]);
        assert_eq!(copies(&entries, "db").len(), 1);
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
        let options = BackupOptions {
            scratch: Some(Scratch::new(&reflink_dir, 0).unwrap()),
            reflink: true,
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
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
        assert_eq!(copies(&entries, "big.bin"), [&[b'a'; 8192][..]]);
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
        let options = BackupOptions {
            scratch: Some(Scratch::new(&reflink_dir, u64::MAX / 2).unwrap()),
            reflink: true,
            retry: fast_retry(3),
            before_read,
            ..BackupOptions::default()
        };

        let (report, entries) = backup_app(temp.path(), &options);
        assert_eq!(report.reflinked, 0);
        assert_eq!(report.retried, 1);
        assert_eq!(copies(&entries, "small.bin"), [&[b'b'; 512][..]]);
    }

    #[test]
    fn missing_scratch_dir_is_reported() {
        let temp = tempfile::tempdir().unwrap();
        assert!(Scratch::new(&temp.path().join("missing"), 0).is_err());
    }
}
