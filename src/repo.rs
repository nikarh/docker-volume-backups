//! Volumes as snapshots in one restic-format repository, written with
//! rustic: files are split into content-defined chunks, and each chunk is
//! stored once, compressed and encrypted, however many volumes and
//! snapshots contain it.

use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use log::info;
use rustic_core::{
    BackupOptions, ConfigOptions, Credentials, KeepOptions, KeyOptions, LocalDestination,
    LsOptions, OpenStatus, ParentOptions, PruneOptions, Repository, RepositoryBackends,
    RepositoryOptions, RestoreOptions, SnapshotOptions, WriteBackend,
    jiff::Zoned,
    repofile::{NodeType, SnapshotFile},
};

use crate::{
    format_human_size,
    source::{ParentFile, ParentFiles, ReadOptions, VolumeReport, VolumeSource},
};

pub struct RepoConfig {
    pub password: String,
    /// Snapshots are identified by this host name and the volume's path.
    pub host: String,
    pub compression_level: Option<i32>,
    /// Local cache of index and snapshot files; none when unset.
    pub cache_dir: Option<PathBuf>,
}

/// An open repository.
pub struct Repo {
    /// Taken out while indexed for a backup or restore, and opened again if
    /// that fails.
    open: RefCell<Option<Repository<OpenStatus>>>,
    backend: Arc<dyn WriteBackend>,
    config: RepoConfig,
}

impl Repo {
    /// Opens the repository in `backend`, creating it if it doesn't exist.
    pub fn open(backend: Arc<dyn WriteBackend>, config: RepoConfig) -> Result<Self> {
        let repo = Self::open_repository(backend.clone(), &config)?;
        Ok(Self {
            open: RefCell::new(Some(repo)),
            backend,
            config,
        })
    }

    fn open_repository(
        backend: Arc<dyn WriteBackend>,
        config: &RepoConfig,
    ) -> Result<Repository<OpenStatus>> {
        let options = match &config.cache_dir {
            Some(dir) => RepositoryOptions::default().cache_dir(dir.clone()),
            None => RepositoryOptions::default().no_cache(true),
        };
        let repo = Repository::new(&options, &RepositoryBackends::new(backend, None))?;
        let credentials = Credentials::Password(config.password.clone());
        if repo.config_id()?.is_some() {
            return repo
                .open(&credentials)
                .context("failed to open the repository");
        }
        info!("initializing a new repository");
        let mut config_options = ConfigOptions::default();
        if let Some(level) = config.compression_level {
            config_options = config_options.set_compression(level);
        }
        repo.init(&credentials, &KeyOptions::default(), &config_options)
            .context("failed to initialize the repository")
    }

    /// Takes the open repository out, to index it. Put it back with
    /// [`Self::put`].
    fn take(&self) -> Result<Repository<OpenStatus>> {
        match self.open.borrow_mut().take() {
            Some(repo) => Ok(repo),
            None => Self::open_repository(self.backend.clone(), &self.config),
        }
    }

    fn put(&self, repo: Repository<OpenStatus>) {
        *self.open.borrow_mut() = Some(repo);
    }

    /// Runs `action` on the open repository.
    fn with<T>(&self, action: impl FnOnce(&Repository<OpenStatus>) -> Result<T>) -> Result<T> {
        let repo = self.take()?;
        let result = action(&repo);
        self.put(repo);
        result
    }

    /// The path a volume is stored under in its snapshots.
    fn snapshot_path(volume: &str) -> String {
        format!("/{volume}")
    }

    fn is_volume_snapshot(&self, snapshot: &SnapshotFile, volume: &str) -> bool {
        let path = Self::snapshot_path(volume);
        snapshot.hostname == self.config.host
            && snapshot.paths.contains(&path)
            && snapshot.paths.iter().count() == 1
    }

    /// Snapshots of `volume`, oldest first.
    pub fn snapshots(&self, volume: &str) -> Result<Vec<SnapshotFile>> {
        let mut snapshots = self.with(|repo| {
            Ok(repo.get_matching_snapshots(|snapshot| self.is_volume_snapshot(snapshot, volume))?)
        })?;
        snapshots.sort_by(|left, right| left.time.cmp(&right.time));
        Ok(snapshots)
    }

    /// Backs up the volume at `source` and returns the new snapshot and what
    /// happened to its files.
    pub fn backup(
        &self,
        source: &Path,
        volume: &str,
        options: &ReadOptions,
    ) -> Result<(SnapshotFile, VolumeReport)> {
        let parent = self.snapshots(volume)?.pop();
        // Indexed again for each volume, so it includes chunks the previous
        // volume added.
        let repo = self.take()?.to_indexed_ids()?;
        let parent_files = match &parent {
            Some(parent) => {
                let mut files = ParentFiles::new();
                let node =
                    repo.node_from_snapshot_and_path(parent, &Self::snapshot_path(volume))?;
                for entry in repo.ls(&node, &LsOptions::default())? {
                    let (path, node) = entry?;
                    if node.node_type == NodeType::File {
                        files.insert(path, ParentFile::from_node(&node));
                    }
                }
                files
            }
            None => ParentFiles::new(),
        };

        let src = VolumeSource::new(source, volume, options.clone(), parent_files)?;
        let report = src.report();
        let parent_opts = match &parent {
            Some(parent) => ParentOptions::default().parents(vec![parent.id.to_string()]),
            None => ParentOptions::default().force(true),
        };
        let backup_options = BackupOptions::default()
            .as_path(PathBuf::from(Self::snapshot_path(volume)))
            .no_scan(true)
            .parent_opts(parent_opts);
        let snapshot =
            SnapshotFile::from_options(&SnapshotOptions::default().host(self.config.host.clone()))?;
        let snapshot = repo.archive(&backup_options, &src, snapshot, &[source.to_owned()])?;
        self.put(repo.drop_index());
        drop(src);
        let report = Arc::try_unwrap(report)
            .map_err(|_| anyhow::anyhow!("volume report still in use"))?
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok((snapshot, report))
    }

    /// Restores `volume` into `target` from the snapshot whose ID starts
    /// with `snapshot`, or from the newest one.
    pub fn restore(&self, volume: &str, target: &Path, snapshot: Option<&str>) -> Result<()> {
        let snapshots = self.snapshots(volume)?;
        let snapshot = match snapshot {
            Some(id) => snapshots
                .into_iter()
                .find(|snapshot| snapshot.id.to_string().starts_with(id))
                .with_context(|| format!("no snapshot {id} of volume {volume}"))?,
            None => snapshots
                .into_iter()
                .next_back()
                .context("no snapshots found")?,
        };
        info!(
            "restoring {volume} from snapshot {} of {}",
            snapshot.id,
            snapshot.time.strftime("%Y-%m-%d %H:%M:%S %Z")
        );

        let repo = self.take()?.to_indexed()?;
        let node = repo.node_from_snapshot_and_path(&snapshot, &Self::snapshot_path(volume))?;
        let ls_options = LsOptions::default();
        let target = format!("{}/", target.display());
        let destination = LocalDestination::new(&target, true, false)?;
        // Containers rarely have the backed-up users and groups, so owners
        // are restored by ID.
        let options = RestoreOptions::default().numeric_id(true);
        let plan =
            repo.prepare_restore(&options, repo.ls(&node, &ls_options)?, &destination, false)?;
        repo.restore(plan, &options, repo.ls(&node, &ls_options)?, &destination)?;
        self.put(repo.drop_index());
        Ok(())
    }

    /// Removes snapshots of `volume` that `keep` doesn't keep. Their data is
    /// freed by [`Self::prune`].
    pub fn forget(&self, volume: &str, keep: &KeepOptions) -> Result<usize> {
        let snapshots = self.snapshots(volume)?;
        let total = snapshots.len();
        let remove = keep
            .apply(snapshots, &Zoned::now())?
            .into_iter()
            .filter(|snapshot| !snapshot.keep)
            .map(|snapshot| snapshot.snapshot.id)
            .collect::<Vec<_>>();
        info!(
            "retention for volume {volume}: found {total} snapshot(s), removing {}, leaving {}",
            remove.len(),
            total - remove.len()
        );
        if !remove.is_empty() {
            self.with(|repo| Ok(repo.delete_snapshots(&remove)?))?;
        }
        Ok(remove.len())
    }

    /// Frees data no snapshot uses anymore. Packs that become unused are
    /// deleted by a later prune, at least a day later, so a backup running
    /// meanwhile can't lose data it relies on.
    pub fn prune(&self) -> Result<()> {
        let options = PruneOptions::default();
        self.with(|repo| {
            let plan = repo.prune_plan(&options)?;
            let stats = &plan.stats;
            info!(
                "prune: deleting {} pack(s) ({}), repacking {}, {} pack(s) kept until a later prune",
                stats.packs_to_delete.remove,
                format_human_size(stats.size_to_delete.remove),
                stats.packs.repack,
                stats.packs_to_delete.keep
            );
            repo.prune(&options, plan)?;
            Ok(())
        })
    }
}

/// Retention as restic's keep options. At least one must be set.
pub fn keep_options(
    last: Option<i32>,
    daily: Option<i32>,
    weekly: Option<i32>,
    monthly: Option<i32>,
) -> Result<KeepOptions> {
    if [last, daily, weekly, monthly].iter().all(Option::is_none) {
        bail!("set at least one of KEEP_LAST, KEEP_DAILY, KEEP_WEEKLY, or KEEP_MONTHLY");
    }
    let mut keep = KeepOptions::default();
    keep.keep_last = last;
    keep.keep_daily = daily;
    keep.keep_weekly = weekly;
    keep.keep_monthly = monthly;
    Ok(keep)
}

pub fn log_backup_completed(
    volume: &str,
    source: &Path,
    snapshot: &SnapshotFile,
    report: &VolumeReport,
    progress: Option<(usize, usize)>,
) {
    let mut details = Vec::new();
    if let Some(summary) = &snapshot.summary {
        details.push(format!(
            "{} added ({} stored)",
            format_human_size(summary.data_added),
            format_human_size(summary.data_added_packed)
        ));
    }
    details.push(format!("{} file(s)", report.files));
    if report.unchanged > 0 {
        details.push(format!("{} unchanged", report.unchanged));
    }
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
    let progress =
        progress.map_or_else(String::new, |(index, total)| format!(" ({index}/{total})"));
    info!(
        "backed up volume {volume}{progress} from {} to snapshot {} ({details})",
        source.display(),
        snapshot.id,
    );
}
