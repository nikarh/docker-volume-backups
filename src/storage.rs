//! Repository storage: a local directory or an SFTP server, laid out the way
//! restic lays out a repository, so restic and rustic can read it too.
//!
//! Files are written under a temporary name and renamed once complete, so an
//! interrupted write never shows up under a repository file name.

use std::{
    fs::{self, File},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use log::{info, warn};
use rustic_core::{
    ALL_FILE_TYPES, BytesList, ErrorKind, FileType, Id, ReadBackend, RusticError, RusticResult,
    WriteBackend,
};
use ssh2::{Session, Sftp};
use url::Url;

/// Buffer between rustic and storage. Large writes let libssh2 pipeline
/// many SFTP packets per round trip instead of waiting for an ack after
/// every few kilobytes.
const WRITE_BUFFER_SIZE: usize = 4 * 1024 * 1024;
/// Suffix of a file that is still being written.
const TEMP_SUFFIX: &str = ".tmp";

/// A file or directory in a storage directory listing.
pub struct Entry {
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
}

/// The few file operations a repository needs, on paths relative to the
/// repository root.
pub trait Files: Send + Sync + 'static {
    fn location(&self) -> String;
    /// Reads the whole file, or `length` bytes from `offset`.
    fn read(&self, path: &str, range: Option<(u64, usize)>) -> io::Result<Vec<u8>>;
    /// Writes `path` under a temporary name, then renames it into place.
    fn write(&self, path: &str, content: &[Bytes]) -> io::Result<()>;
    /// Lists a directory; a missing directory is empty.
    fn list(&self, dir: &str) -> io::Result<Vec<Entry>>;
    fn create_dir_all(&self, dir: &str) -> io::Result<()>;
    fn remove(&self, path: &str) -> io::Result<()>;
}

/// A rustic repository backend on top of [`Files`].
pub struct Backend<F> {
    files: F,
}

impl<F: Files> Backend<F> {
    pub fn new(files: F) -> Self {
        Self { files }
    }

    /// Removes files left behind by interrupted writes. Only call this while
    /// nothing else writes to the repository.
    pub fn remove_temp_files(&self) -> Result<usize> {
        let mut dirs = vec![String::new()];
        dirs.extend(
            ALL_FILE_TYPES
                .iter()
                .filter(|tpe| **tpe != FileType::Config && **tpe != FileType::Pack)
                .map(|tpe| tpe.dirname().to_owned()),
        );
        dirs.extend((0..=255u8).map(|byte| format!("data/{byte:02x}")));
        let mut removed = 0;
        for dir in dirs {
            for entry in self.files.list(&dir)? {
                if !entry.is_dir && entry.name.ends_with(TEMP_SUFFIX) {
                    let path = join(&dir, &entry.name);
                    info!("removing leftover partial file {path}");
                    match self.files.remove(&path) {
                        Ok(()) => removed += 1,
                        Err(error) => warn!("failed to remove {path}: {error}"),
                    }
                }
            }
        }
        Ok(removed)
    }
}

/// Where restic keeps a file, relative to the repository root.
fn file_path(tpe: FileType, id: &Id) -> String {
    let hex = id.to_hex().to_string();
    match tpe {
        FileType::Config => "config".to_owned(),
        FileType::Pack => format!("data/{}/{hex}", &hex[..2]),
        _ => format!("{}/{hex}", tpe.dirname()),
    }
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_owned()
    } else {
        format!("{dir}/{name}")
    }
}

/// A unique temporary name next to `path`.
fn temp_path(path: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{path}.{}-{n}{TEMP_SUFFIX}", process::id())
}

fn parent_dir(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(dir, _)| dir)
}

fn backend_error(action: &str, path: &str, error: io::Error) -> Box<RusticError> {
    RusticError::with_source(
        ErrorKind::Backend,
        format!("Failed to {action} `{{path}}`."),
        error,
    )
    .attach_context("path", path.to_owned())
}

impl<F: Files> ReadBackend for Backend<F> {
    fn location(&self) -> String {
        self.files.location()
    }

    fn list_with_size(&self, tpe: FileType) -> RusticResult<Vec<(Id, u32)>> {
        let dirs = match tpe {
            FileType::Config => {
                let entries = self
                    .files
                    .list("")
                    .map_err(|error| backend_error("list", "/", error))?;
                return Ok(entries
                    .into_iter()
                    .filter(|entry| !entry.is_dir && entry.name == "config")
                    .map(|entry| (Id::default(), u32::try_from(entry.size).unwrap_or(u32::MAX)))
                    .collect());
            }
            FileType::Pack => self
                .files
                .list("data")
                .map_err(|error| backend_error("list", "data", error))?
                .into_iter()
                .filter(|entry| entry.is_dir)
                .map(|entry| join("data", &entry.name))
                .collect(),
            _ => vec![tpe.dirname().to_owned()],
        };
        let mut files = Vec::new();
        for dir in dirs {
            for entry in self
                .files
                .list(&dir)
                .map_err(|error| backend_error("list", &dir, error))?
            {
                if entry.is_dir {
                    continue;
                }
                let Ok(id) = entry.name.parse::<Id>() else {
                    continue;
                };
                match u32::try_from(entry.size) {
                    Ok(size) => files.push((id, size)),
                    Err(_) => warn!("ignoring {dir}/{}: too large", entry.name),
                }
            }
        }
        Ok(files)
    }

    fn read_full(&self, tpe: FileType, id: &Id) -> RusticResult<Bytes> {
        let path = file_path(tpe, id);
        self.files
            .read(&path, None)
            .map(Bytes::from)
            .map_err(|error| backend_error("read", &path, error))
    }

    fn read_partial(
        &self,
        tpe: FileType,
        id: &Id,
        _cacheable: bool,
        offset: u32,
        length: u32,
    ) -> RusticResult<Bytes> {
        let path = file_path(tpe, id);
        let length = usize::try_from(length).unwrap_or(usize::MAX);
        self.files
            .read(&path, Some((offset.into(), length)))
            .map(Bytes::from)
            .map_err(|error| backend_error("read", &path, error))
    }

    fn warmup_path(&self, tpe: FileType, id: &Id) -> String {
        file_path(tpe, id)
    }
}

impl<F: Files> WriteBackend for Backend<F> {
    fn create(&self) -> RusticResult<()> {
        let mut dirs = vec![String::new()];
        dirs.extend(
            ALL_FILE_TYPES
                .iter()
                .filter(|tpe| **tpe != FileType::Config)
                .map(|tpe| tpe.dirname().to_owned()),
        );
        dirs.extend((0..=255u8).map(|byte| format!("data/{byte:02x}")));
        for dir in dirs {
            self.files
                .create_dir_all(&dir)
                .map_err(|error| backend_error("create directory", &dir, error))?;
        }
        Ok(())
    }

    fn write_bytes(
        &self,
        tpe: FileType,
        id: &Id,
        _cacheable: bool,
        content: BytesList,
    ) -> RusticResult<()> {
        let path = file_path(tpe, id);
        self.files
            .write(&path, content.slice())
            .map_err(|error| backend_error("write", &path, error))
    }

    fn remove(&self, tpe: FileType, id: &Id, _cacheable: bool) -> RusticResult<()> {
        let path = file_path(tpe, id);
        self.files
            .remove(&path)
            .map_err(|error| backend_error("remove", &path, error))
    }
}

/// A repository in a local directory, such as a mounted backup disk.
pub struct LocalFiles {
    root: PathBuf,
}

impl LocalFiles {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path(&self, path: &str) -> PathBuf {
        if path.is_empty() {
            self.root.clone()
        } else {
            self.root.join(path)
        }
    }
}

impl Files for LocalFiles {
    fn location(&self) -> String {
        format!("local:{}", self.root.display())
    }

    fn read(&self, path: &str, range: Option<(u64, usize)>) -> io::Result<Vec<u8>> {
        let mut file = File::open(self.path(path))?;
        match range {
            None => {
                let mut data = Vec::new();
                file.read_to_end(&mut data)?;
                Ok(data)
            }
            Some((offset, length)) => {
                file.seek(SeekFrom::Start(offset))?;
                let mut data = vec![0; length];
                file.read_exact(&mut data)?;
                Ok(data)
            }
        }
    }

    fn write(&self, path: &str, content: &[Bytes]) -> io::Result<()> {
        let target = self.path(path);
        let temp = self.path(&temp_path(path));
        if let Some(dir) = target.parent() {
            fs::create_dir_all(dir)?;
        }
        let result = (|| {
            let mut file = BufWriter::with_capacity(WRITE_BUFFER_SIZE, File::create(&temp)?);
            for bytes in content {
                file.write_all(bytes)?;
            }
            file.into_inner()?.sync_all()?;
            fs::rename(&temp, &target)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
    }

    fn list(&self, dir: &str) -> io::Result<Vec<Entry>> {
        let entries = match fs::read_dir(self.path(dir)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut list = Vec::new();
        for entry in entries {
            let entry = entry?;
            let metadata = entry.metadata()?;
            list.push(Entry {
                name: entry.file_name().to_string_lossy().into_owned(),
                size: metadata.len(),
                is_dir: metadata.is_dir(),
            });
        }
        Ok(list)
    }

    fn create_dir_all(&self, dir: &str) -> io::Result<()> {
        fs::create_dir_all(self.path(dir))
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        fs::remove_file(self.path(path))
    }
}

/// How to reach and log in to an SFTP server.
pub struct SftpConfig {
    pub url: String,
    pub password: Option<String>,
    pub private_key: Option<String>,
    pub private_key_passphrase: Option<String>,
}

/// A repository on an SFTP server, reached with libssh2, so no `ssh`
/// binary is needed.
pub struct SftpFiles {
    sftp: Sftp,
    root: String,
    location: String,
}

impl SftpFiles {
    pub fn connect(config: &SftpConfig) -> Result<Self> {
        let parsed = Url::parse(&config.url).context("failed to parse SFTP_URL")?;
        if parsed.scheme() != "ssh" && parsed.scheme() != "sftp" {
            bail!("SFTP_URL must use ssh:// or sftp://");
        }

        let host = parsed.host_str().context("SFTP_URL must include a host")?;
        let port = parsed.port().unwrap_or(22);
        let username = if parsed.username().is_empty() {
            std::env::var("USER").unwrap_or_else(|_| "backup".to_owned())
        } else {
            parsed.username().to_owned()
        };
        let root = normalize_remote_dir(parsed.path());

        let tcp = TcpStream::connect((host, port))
            .with_context(|| format!("failed to connect to {host}:{port}"))?;
        let mut session = Session::new().context("failed to create SSH session")?;
        session.set_tcp_stream(tcp);
        session.handshake().context("SSH handshake failed")?;

        match (config.private_key.as_deref(), config.password.as_deref()) {
            (Some(key), _) => session
                .userauth_pubkey_memory(
                    &username,
                    None,
                    key,
                    config.private_key_passphrase.as_deref(),
                )
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
        Ok(Self {
            sftp,
            location: format!("sftp:{username}@{host}:{port}{root}"),
            root,
        })
    }

    fn path(&self, path: &str) -> PathBuf {
        PathBuf::from(if path.is_empty() {
            self.root.clone()
        } else if self.root == "/" {
            format!("/{path}")
        } else {
            format!("{}/{path}", self.root)
        })
    }

    fn write_file(&self, temp: &Path, content: &[Bytes]) -> io::Result<()> {
        let mut file = BufWriter::with_capacity(WRITE_BUFFER_SIZE, self.sftp.create(temp)?);
        for bytes in content {
            file.write_all(bytes)?;
        }
        let mut file = file.into_inner().map_err(io::IntoInnerError::into_error)?;
        // Servers without the fsync extension still close the file cleanly.
        let _ = file.fsync();
        Ok(())
    }
}

impl Files for SftpFiles {
    fn location(&self) -> String {
        self.location.clone()
    }

    fn read(&self, path: &str, range: Option<(u64, usize)>) -> io::Result<Vec<u8>> {
        let mut file = self.sftp.open(self.path(path)).map_err(sftp_error)?;
        match range {
            None => {
                let mut data = Vec::new();
                file.read_to_end(&mut data)?;
                Ok(data)
            }
            Some((offset, length)) => {
                file.seek(SeekFrom::Start(offset))?;
                let mut data = vec![0; length];
                file.read_exact(&mut data)?;
                Ok(data)
            }
        }
    }

    fn write(&self, path: &str, content: &[Bytes]) -> io::Result<()> {
        let target = self.path(path);
        let temp = self.path(&temp_path(path));
        let mut written = self.write_file(&temp, content);
        if written
            .as_ref()
            .is_err_and(|error| error.kind() == io::ErrorKind::NotFound)
        {
            // The directory is missing, such as data/xx in a repository
            // created elsewhere.
            self.create_dir_all(parent_dir(path))?;
            written = self.write_file(&temp, content);
        }
        let result = written.and_then(|()| {
            match self.sftp.rename(&temp, &target, None) {
                Ok(()) => Ok(()),
                // SFTP v3 servers refuse to rename over an existing file.
                Err(_) if self.sftp.stat(&target).is_ok() => {
                    self.sftp.unlink(&target).map_err(sftp_error)?;
                    self.sftp.rename(&temp, &target, None).map_err(sftp_error)
                }
                Err(error) => Err(sftp_error(error)),
            }
        });
        if result.is_err() {
            let _ = self.sftp.unlink(&temp);
        }
        result
    }

    fn list(&self, dir: &str) -> io::Result<Vec<Entry>> {
        let entries = match self.sftp.readdir(self.path(dir)) {
            Ok(entries) => entries,
            Err(error) if is_missing_remote_entry(&error) => return Ok(Vec::new()),
            Err(error) => return Err(sftp_error(error)),
        };
        Ok(entries
            .into_iter()
            .filter_map(|(path, stat)| {
                let name = path.file_name()?.to_str()?.to_owned();
                let file_type = stat.file_type();
                (file_type.is_dir() || file_type.is_file()).then(|| Entry {
                    name,
                    size: stat.size.unwrap_or(0),
                    is_dir: file_type.is_dir(),
                })
            })
            .collect())
    }

    fn create_dir_all(&self, dir: &str) -> io::Result<()> {
        let full = self.path(dir);
        let mut current = PathBuf::from("/");
        for part in full.components().skip(1) {
            current.push(part);
            match self.sftp.stat(&current) {
                Ok(stat) if stat.file_type().is_dir() => {}
                Ok(_) => {
                    return Err(io::Error::other(format!(
                        "{} exists but is not a directory",
                        current.display()
                    )));
                }
                Err(error) if is_missing_remote_entry(&error) => {
                    if let Err(error) = self.sftp.mkdir(&current, 0o755) {
                        // Created concurrently by another writer.
                        if !self
                            .sftp
                            .stat(&current)
                            .is_ok_and(|stat| stat.file_type().is_dir())
                        {
                            return Err(sftp_error(error));
                        }
                    }
                }
                Err(error) => return Err(sftp_error(error)),
            }
        }
        Ok(())
    }

    fn remove(&self, path: &str) -> io::Result<()> {
        self.sftp.unlink(&self.path(path)).map_err(sftp_error)
    }
}

fn normalize_remote_dir(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        "/".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn is_missing_remote_entry(error: &ssh2::Error) -> bool {
    matches!(
        error.code(),
        ssh2::ErrorCode::Session(-16) | ssh2::ErrorCode::SFTP(2 | 10)
    )
}

fn sftp_error(error: ssh2::Error) -> io::Error {
    if is_missing_remote_entry(&error) {
        io::Error::new(io::ErrorKind::NotFound, error)
    } else {
        io::Error::other(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn files_are_laid_out_like_restic() {
        let id: Id = "8fa5c6a3b1f2d4e5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9"
            .parse()
            .unwrap();
        assert_eq!(file_path(FileType::Config, &id), "config");
        assert_eq!(
            file_path(FileType::Pack, &id),
            format!("data/8f/{}", id.to_hex().as_str())
        );
        assert_eq!(
            file_path(FileType::Snapshot, &id),
            format!("snapshots/{}", id.to_hex().as_str())
        );
    }

    #[test]
    fn interrupted_writes_leave_only_temp_files_that_cleanup_removes() {
        let temp = tempfile::tempdir().unwrap();
        let backend = Backend::new(LocalFiles::new(temp.path().to_owned()));
        backend.create().unwrap();
        let id = Id::default();
        backend
            .write_bytes(
                FileType::Snapshot,
                &id,
                false,
                BytesList::from(Bytes::from_static(b"snapshot")),
            )
            .unwrap();
        fs::write(temp.path().join("data/00/abc.123-0.tmp"), "partial").unwrap();

        assert_eq!(backend.list(FileType::Snapshot).unwrap(), [id]);
        assert!(backend.list(FileType::Pack).unwrap().is_empty());
        assert_eq!(backend.remove_temp_files().unwrap(), 1);
        assert_eq!(
            &backend
                .read_partial(FileType::Snapshot, &id, false, 4, 4)
                .unwrap()[..],
            b"shot"
        );
    }
}
