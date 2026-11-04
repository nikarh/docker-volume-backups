mod repo;
mod source;
mod storage;

use std::{
    env, fs, io,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Local};
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use cron::Schedule;
use log::{error, info, warn};
use rustic_core::{KeepOptions, WriteBackend};

use crate::{
    repo::{Repo, RepoConfig, keep_options, log_backup_completed},
    source::{ReadOptions, RetryPolicy, Scratch},
    storage::{Backend, LocalFiles, SftpConfig, SftpFiles},
};

const DEFAULT_VOLUMES_ROOT: &str = "/volumes";
const DEFAULT_LOCAL_STORAGE_ROOT: &str = "/backups";
const DEFAULT_BACKUP_CRON: &str = "0 1 * * *";
const DEFAULT_REPOSITORY_PASSWORD_FILE: &str = "/run/secrets/REPOSITORY_PASSWORD";
const DEFAULT_SFTP_PASSWORD_FILE: &str = "/run/secrets/SFTP_PASSWORD";
const DEFAULT_SFTP_PRIVATE_KEY_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY";
const DEFAULT_SFTP_PRIVATE_KEY_PASSPHRASE_FILE: &str = "/run/secrets/SFTP_PRIVATE_KEY_PASSPHRASE";
const DEFAULT_SCRATCH_DIR: &str = "/scratch";
const DEFAULT_SNAPSHOT_HOST: &str = "docker-volume-backups";

#[derive(Debug, Parser)]
#[command(author, version, about, subcommand_required = true)]
struct Cli {
    #[arg(long, env = "VOLUMES_ROOT", default_value = DEFAULT_VOLUMES_ROOT)]
    volumes_root: PathBuf,

    #[arg(long, env = "STORAGE_DRIVER", default_value = "local")]
    storage_driver: StorageDriverKind,

    /// Repository directory for the `local` driver.
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

    /// Password that encrypts the repository. Without it, backups can't be
    /// restored.
    #[arg(long, env = "REPOSITORY_PASSWORD")]
    repository_password: Option<String>,

    #[arg(
        long,
        env = "REPOSITORY_PASSWORD_FILE",
        default_value = DEFAULT_REPOSITORY_PASSWORD_FILE
    )]
    repository_password_file: PathBuf,

    /// Host name recorded in snapshots. Servers sharing a repository need
    /// different ones.
    #[arg(long, env = "SNAPSHOT_HOST", default_value = DEFAULT_SNAPSHOT_HOST)]
    snapshot_host: String,

    /// zstd level (-7 to 22) of a new repository; rustic's default when unset.
    #[arg(long, env = "COMPRESSION_LEVEL", allow_hyphen_values = true)]
    compression_level: Option<i32>,

    /// Writable directory caching index and snapshot files between runs, so
    /// they aren't all downloaded again. No cache when unset.
    #[arg(long, env = "CACHE_DIR")]
    cache_dir: Option<PathBuf>,

    /// Snapshots to keep per volume: the newest ones.
    #[arg(long, env = "KEEP_LAST", default_value = "7")]
    keep_last: Option<i32>,

    /// Also keep the newest snapshot of each of the last N days.
    #[arg(long, env = "KEEP_DAILY")]
    keep_daily: Option<i32>,

    /// Also keep the newest snapshot of each of the last N weeks.
    #[arg(long, env = "KEEP_WEEKLY")]
    keep_weekly: Option<i32>,

    /// Also keep the newest snapshot of each of the last N months.
    #[arg(long, env = "KEEP_MONTHLY")]
    keep_monthly: Option<i32>,

    /// Archive each file from a reflink copy (btrfs, XFS), so it can't change while it is read.
    #[arg(long, env = "REFLINK", default_value = "auto")]
    reflink: ReflinkMode,

    /// Writable directory for temporary copies of files: reflinks when it's
    /// on the volumes' filesystem and that supports them, else full copies.
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

    /// Reads of a file that keeps changing before its last read is archived
    /// as possibly inconsistent. 1 disables retries.
    #[arg(long, env = "RETRY_ATTEMPTS", default_value_t = 3)]
    retry_attempts: u32,

    /// Wait before reading a changed file again.
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
    /// larger ones are copied into `SCRATCH_DIR`.
    #[arg(
        long,
        env = "RETRY_BUFFER_SIZE",
        default_value = "16MiB",
        value_parser = parse_size
    )]
    retry_buffer_size: u64,

    /// Large files are copied before reading only if they changed within
    /// this time. Older ones are streamed directly.
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
enum ReflinkMode {
    /// Reflink when `SCRATCH_DIR` is usable and the filesystem supports it.
    Auto,
    /// Always read the live files.
    Off,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Back up one volume.
    Backup { volume: String },
    /// Restore one volume, defaulting to the newest snapshot.
    Restore {
        volume: String,
        /// Snapshot ID or a unique prefix of it.
        #[arg(long)]
        snapshot: Option<String>,
    },
    /// Restore every subdirectory under `VOLUMES_ROOT` from its newest snapshot.
    RestoreAll,
    /// Back up every subdirectory under `VOLUMES_ROOT`.
    BackupAll,
    /// List the snapshots of one volume, or of every discovered volume.
    Snapshots { volume: Option<String> },
    /// Apply the retention policy to every discovered volume and free unused data.
    Cleanup,
    /// Run the cron scheduler.
    #[command(alias = "run")]
    Schedule {
        /// Cron expression for scheduled backup runs.
        #[arg(long = "backup-cron", env = "BACKUP_CRON", default_value = DEFAULT_BACKUP_CRON)]
        backup_cron: String,
    },
}

/// Where the repository is stored.
enum Store {
    Local(Arc<Backend<LocalFiles>>),
    Sftp(Arc<Backend<SftpFiles>>),
}

impl Store {
    fn connect(config: &Cli) -> Result<Self> {
        Ok(match config.storage_driver {
            StorageDriverKind::Local => Self::Local(Arc::new(Backend::new(LocalFiles::new(
                config.local_storage_root.clone(),
            )))),
            StorageDriverKind::Sftp => {
                let url = config
                    .sftp_url
                    .clone()
                    .context("SFTP_URL is required when STORAGE_DRIVER=sftp")?;
                let sftp = SftpConfig {
                    url,
                    password: secret_value(
                        config.sftp_password.as_ref(),
                        &config.sftp_password_file,
                    )?,
                    private_key: secret_value(
                        config.sftp_private_key.as_ref(),
                        &config.sftp_private_key_file,
                    )?,
                    private_key_passphrase: secret_value(
                        config.sftp_private_key_passphrase.as_ref(),
                        &config.sftp_private_key_passphrase_file,
                    )?,
                };
                Self::Sftp(Arc::new(Backend::new(SftpFiles::connect(&sftp)?)))
            }
        })
    }

    fn backend(&self) -> Arc<dyn WriteBackend> {
        match self {
            Self::Local(backend) => backend.clone(),
            Self::Sftp(backend) => backend.clone(),
        }
    }

    fn remove_temp_files(&self) -> Result<usize> {
        match self {
            Self::Local(backend) => backend.remove_temp_files(),
            Self::Sftp(backend) => backend.remove_temp_files(),
        }
    }
}

fn open_repo(config: &Cli) -> Result<(Store, Repo)> {
    let store = Store::connect(config)?;
    let password = secret_value(
        config.repository_password.as_ref(),
        &config.repository_password_file,
    )?
    .context("REPOSITORY_PASSWORD or REPOSITORY_PASSWORD_FILE is required")?;
    let repo = Repo::open(
        store.backend(),
        RepoConfig {
            password,
            host: config.snapshot_host.clone(),
            compression_level: config.compression_level,
            cache_dir: config.cache_dir.clone(),
        },
    )?;
    Ok((store, repo))
}

fn read_options(config: &Cli) -> ReadOptions {
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
    ReadOptions {
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

fn retention(config: &Cli) -> Result<KeepOptions> {
    keep_options(
        config.keep_last,
        config.keep_daily,
        config.keep_weekly,
        config.keep_monthly,
    )
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
            let (_, repo) = open_repo(&cli)?;
            backup_volume(&cli.volumes_root, &repo, &read_options(&cli), volume, None)?;
        }
        Command::Restore { volume, snapshot } => {
            info!("executing restore command for volume {volume}");
            let (_, repo) = open_repo(&cli)?;
            restore_volume(&cli.volumes_root, &repo, volume, snapshot.as_deref())?;
        }
        Command::RestoreAll => {
            info!("executing restore-all command");
            let (_, repo) = open_repo(&cli)?;
            restore_all(&cli.volumes_root, &repo)?;
        }
        Command::BackupAll => {
            info!("executing backup-all command");
            let (_, repo) = open_repo(&cli)?;
            backup_all(&cli.volumes_root, &repo, &read_options(&cli))?.into_result("backup")?;
        }
        Command::Snapshots { volume } => {
            let (_, repo) = open_repo(&cli)?;
            let volumes = match volume {
                Some(volume) => vec![volume.clone()],
                None => discover_volumes(&cli.volumes_root)?,
            };
            print_snapshots(&repo, &volumes)?;
        }
        Command::Cleanup => {
            info!("executing cleanup command");
            let (store, repo) = open_repo(&cli)?;
            let volumes = discover_volumes(&cli.volumes_root)?;
            cleanup(&store, &repo, &volumes, &retention(&cli)?)?;
        }
        Command::Schedule { backup_cron } => {
            info!("executing schedule command");
            run_scheduler(&cli, backup_cron)?;
        }
    }

    Ok(())
}

fn validate_config(config: &Cli) -> Result<()> {
    if config.retry_attempts == 0 {
        bail!("RETRY_ATTEMPTS must be greater than 0");
    }
    if let Some(level) = config.compression_level
        && !(-7..=22).contains(&level)
    {
        bail!("COMPRESSION_LEVEL {level} is out of range -7 to 22");
    }
    retention(config)?;
    Ok(())
}

fn backup_volume(
    volumes_root: &Path,
    repo: &Repo,
    options: &ReadOptions,
    volume: &str,
    progress: Option<(usize, usize)>,
) -> Result<()> {
    validate_volume_name(volume)?;
    let source = volumes_root.join(volume);
    if !source.is_dir() {
        bail!("volume {volume} does not exist at {}", source.display());
    }
    let (snapshot, report) = repo.backup(&source, volume, options)?;
    log_backup_completed(volume, &source, &snapshot, &report, progress);
    Ok(())
}

fn restore_volume(
    volumes_root: &Path,
    repo: &Repo,
    volume: &str,
    snapshot: Option<&str>,
) -> Result<()> {
    validate_volume_name(volume)?;
    let target = volumes_root.join(volume);
    fs::create_dir_all(&target)
        .with_context(|| format!("failed to create volume directory {}", target.display()))?;
    repo.restore(volume, &target, snapshot)
}

/// Backs up every volume, continuing past volumes that fail. Returns the
/// volumes that were backed up and the ones that failed.
fn backup_all(volumes_root: &Path, repo: &Repo, options: &ReadOptions) -> Result<VolumeResults> {
    let volumes = discover_volumes(volumes_root)?;
    info!("backing up {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        let progress = (index + 1, volumes.len());
        match backup_volume(volumes_root, repo, options, volume, Some(progress)) {
            Ok(()) => results.succeeded.push(volume.clone()),
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

fn restore_all(volumes_root: &Path, repo: &Repo) -> Result<()> {
    let volumes = discover_volumes(volumes_root)?;
    info!("restoring {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "restoring volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        match restore_volume(volumes_root, repo, volume, None) {
            Ok(()) => results.succeeded.push(volume.clone()),
            Err(error) => {
                error!("restore of volume {volume} failed: {error:#}");
                results.failed.push(volume.clone());
            }
        }
    }
    results.into_result("restore")
}

fn print_snapshots(repo: &Repo, volumes: &[String]) -> Result<()> {
    for volume in volumes {
        println!("{volume}:");
        for snapshot in repo.snapshots(volume)? {
            let size = snapshot
                .summary
                .as_ref()
                .map_or(0, |summary| summary.total_bytes_processed);
            println!(
                "  {}  {}  {}",
                &snapshot.id.to_string()[..8],
                snapshot.time.strftime("%Y-%m-%d %H:%M:%S %Z"),
                format_human_size(size)
            );
        }
    }
    Ok(())
}

/// Applies retention to each volume, continuing past volumes that fail,
/// then frees the data no snapshot uses anymore.
fn cleanup(store: &Store, repo: &Repo, volumes: &[String], keep: &KeepOptions) -> Result<()> {
    info!("cleaning up {} volume(s)", volumes.len());
    let mut results = VolumeResults::default();
    for (index, volume) in volumes.iter().enumerate() {
        info!(
            "cleaning up volume {volume} ({}/{})",
            index + 1,
            volumes.len()
        );
        match repo.forget(volume, keep) {
            Ok(_) => results.succeeded.push(volume.clone()),
            Err(error) => {
                error!("cleanup of volume {volume} failed: {error:#}");
                results.failed.push(volume.clone());
            }
        }
    }
    // Leftovers of interrupted writes. Cleanup runs after backups finish, so
    // nothing is writing them anymore.
    let prune = store
        .remove_temp_files()
        .and_then(|_| repo.prune())
        .context("prune failed");
    results.into_result("cleanup")?;
    prune
}

fn run_scheduler(config: &Cli, backup_cron: &str) -> Result<()> {
    let schedule = parse_schedule(backup_cron)?;
    let keep = retention(config)?;
    info!("cron scheduler started");
    info!("cron expression: {backup_cron}");
    let mut next = next_scheduled_run(&schedule)?;
    info!("next backup run at {next}");

    loop {
        sleep_until_run(next);

        let result = (|| -> Result<()> {
            let (store, repo) = open_repo(config)?;
            let results = backup_all(&config.volumes_root, &repo, &read_options(config))?;
            // Only rotate snapshots of volumes that just got a fresh backup,
            // so a failing volume keeps all of its older snapshots.
            let cleanup = cleanup(&store, &repo, &results.succeeded, &keep);
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
    thread::sleep(sleep_for);
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

#[cfg(test)]
mod tests;
