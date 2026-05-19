use std::{
    env,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    str::FromStr,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local, Utc};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use cron::Schedule;
use filetime::FileTime;
use log::{info, warn};
use ssh2::{FileStat, Session, Sftp};
use tar::{Archive, Builder, EntryType, Header};
use url::Url;
use walkdir::WalkDir;
use xz2::{read::XzDecoder, write::XzEncoder};

const DEFAULT_VOLUMES_ROOT: &str = "/volumes";
const DEFAULT_LOCAL_STORAGE_ROOT: &str = "/backups";
const DEFAULT_BACKUP_CRON: &str = "0 1 * * *";
const DEFAULT_SFTP_PASSWORD_FILE: &str = "/run/secrets/SFTP_PASSWORD";
const DEFAULT_SFTP_PRIVATE_KEY_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY";
const DEFAULT_SFTP_PRIVATE_KEY_PASSPHRASE_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY_PASSPHRASE";

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

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, ValueEnum)]
enum StorageDriverKind {
    Local,
    Sftp,
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
    fn list(&mut self, volume: &str) -> Result<Vec<ArchiveInfo>>;
    fn remove(&mut self, volume: &str, archive: &str) -> Result<()>;
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
        Ok(Box::new(BufWriter::new(file)))
    }

    fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>> {
        let path = self.volume_dir(volume).join(archive);
        let file = File::open(&path)
            .with_context(|| format!("failed to open archive {}", path.display()))?;
        Ok(Box::new(BufReader::new(file)))
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
            if is_archive_name(&name) {
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
        Ok(Box::new(file))
    }

    fn get(&mut self, volume: &str, archive: &str) -> Result<Box<dyn Read + '_>> {
        let path = self.path(volume, Some(archive));
        let file = self
            .sftp
            .open(Path::new(&path))
            .with_context(|| format!("failed to open remote archive {path}"))?;
        Ok(Box::new(file))
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
            if !is_regular_file(&stat) || !is_archive_name(name) {
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
            backup_volume(&cli.volumes_root, storage.as_mut(), volume, None)?;
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
            backup_all(&cli.volumes_root, storage.as_mut())?;
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
    Ok(())
}

fn backup_volume(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    volume: &str,
    progress: Option<(usize, usize)>,
) -> Result<ArchiveInfo> {
    validate_volume_name(volume)?;
    let source = volumes_root.join(volume);
    if !source.is_dir() {
        bail!("volume {volume} does not exist at {}", source.display());
    }

    let archive = format!("{}.tar.xz", Utc::now().format("%Y-%m-%dT%H-%M-%SZ"));
    let writer = CountingWriter::new(storage.put(volume, &archive)?);
    let encoder = XzEncoder::new(writer, 6);
    let mut tar = Builder::new(encoder);
    append_directory_contents(&mut tar, &source)?;
    let encoder = tar.into_inner().context("failed to finish tar stream")?;
    let mut writer = encoder.finish().context("failed to finish xz stream")?;
    writer.flush().context("failed to flush archive stream")?;
    let archive_info = ArchiveInfo {
        name: archive,
        size: writer.bytes_written(),
    };
    log_backup_completed(volume, &source, &archive_info, progress);
    Ok(archive_info)
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
    let reader = storage.get(volume, &archive)?;
    let decoder = XzDecoder::new(reader);
    let mut tar = Archive::new(decoder);
    tar.unpack(&target)
        .with_context(|| format!("failed to unpack archive into {}", target.display()))
}

fn backup_all(volumes_root: &Path, storage: &mut dyn Storage) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    info!("backing up {} volume(s)", volumes.len());
    for (index, volume) in volumes.iter().enumerate() {
        backup_volume(
            volumes_root,
            storage,
            volume,
            Some((index + 1, volumes.len())),
        )?;
    }
    Ok(())
}

fn restore_all(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    archive: Option<&str>,
) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    info!("restoring {} volume(s)", volumes.len());
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "restoring volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        restore_volume(volumes_root, storage, volume, archive)?;
    }
    Ok(())
}

fn cleanup_all(
    volumes_root: &Path,
    storage: &mut dyn Storage,
    policy: &RetentionPolicy,
) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    info!("cleaning up {} volume(s)", volumes.len());
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "cleaning up volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        cleanup_volume(storage, volume, policy)?;
    }
    Ok(())
}

fn cleanup_volume(storage: &mut dyn Storage, volume: &str, policy: &RetentionPolicy) -> Result<()> {
    let archives = storage.list(volume)?;
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

    for archive in to_remove {
        info!("removing {volume}/{archive}");
        storage.remove(volume, &archive)?;
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
            backup_all(&config.volumes_root, storage.as_mut())?;
            cleanup_all(
                &config.volumes_root,
                storage.as_mut(),
                &retention_policy(config),
            )
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

fn append_directory_contents<W: Write>(tar: &mut Builder<W>, root: &Path) -> Result<()> {
    for entry in WalkDir::new(root).follow_links(false).sort_by_file_name() {
        let entry = entry?;
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .with_context(|| format!("failed to make {} relative", path.display()))?;
        if relative.as_os_str().is_empty() {
            continue;
        }

        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            tar.append_dir(relative, path)?;
        } else if metadata.is_file() {
            let mut file = File::open(path)?;
            tar.append_file(relative, &mut file)?;
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(path)?;
            let mut header = Header::new_gnu();
            header.set_entry_type(EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            if let Ok(modified) = metadata.modified() {
                set_header_mtime(&mut header, modified);
            }
            header.set_cksum();
            tar.append_link(&mut header, relative, target)?;
        }
    }
    Ok(())
}

fn discover_volumes(root: &Path) -> Result<Vec<String>> {
    let mut volumes = Vec::new();
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry = entry?;
        if !entry.metadata()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        validate_volume_name(&name)?;
        volumes.push(name);
    }
    volumes.sort();
    Ok(volumes)
}

fn latest_archive(storage: &mut dyn Storage, volume: &str) -> Result<Option<String>> {
    Ok(storage.list(volume)?.pop().map(|archive| archive.name))
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
    progress: Option<(usize, usize)>,
) {
    let size = format_human_size(archive.size);
    match progress {
        Some((index, total)) => info!(
            "backed up volume {volume} ({index}/{total}) from {} to archive {} ({size})",
            source.display(),
            archive.name
        ),
        None => info!(
            "backed up volume {volume} from {} to archive {} ({size})",
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

fn is_archive_name(name: &str) -> bool {
    name.ends_with(".tar.xz")
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
        backup_all(&volumes_root, &mut storage).unwrap();

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
        let archive = backup_volume(&volumes_root, &mut storage, "app", None).unwrap();

        assert!(archive.size > 0);
        assert_eq!(storage.list("app").unwrap().len(), 1);
        assert!(backups_root.join("app").is_dir());
    }
}
