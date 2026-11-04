# docker-volume-backups

`docker-volume-backups` is a small Rust backup tool for Dockerized file servers. It discovers volumes mounted as subdirectories, backs each one up as a snapshot in a deduplicating, encrypted [restic](https://restic.net)-format repository on local or SFTP storage, restores a single volume, and removes old snapshots using a retention policy.

The repository is written with [rustic_core](https://github.com/rustic-rs/rustic_core), so `restic` and `rustic` can list, check, and restore it too. The image is still a single static binary on `scratch`: SFTP goes through libssh2, so no `ssh` binary is needed.

The container defaults to `docker-volume-backups schedule`, which waits for `BACKUP_CRON` and then runs `backup-all` followed by `cleanup`. The default schedule is daily at 01:00 local time:

```cron
0 1 * * *
```

## How backups are stored

All volumes share one repository. Files are split into chunks at content-defined boundaries, so an edit in the middle of a file changes only the chunks around it. Each chunk is identified by its hash, compressed with zstd, encrypted, and stored once, however many snapshots or volumes contain it. A nightly backup of a volume that barely changed adds a few megabytes, not another full copy.

A file whose size, modification time, and change time match the previous snapshot of its volume isn't read at all; the snapshot reuses its chunks.

Each volume's snapshots are recorded under the path `/<volume>` and the host name `SNAPSHOT_HOST`. Servers sharing one repository need different `SNAPSHOT_HOST` values.

The repository is encrypted with `REPOSITORY_PASSWORD`. **Without the password, nothing can be restored**, so keep a copy outside the server. The repository is created on the first run.

## Storage

Storage is selected with `STORAGE_DRIVER`.

`local` keeps the repository in `LOCAL_STORAGE_ROOT`. This is useful when a writable backup disk is mounted into the container.

`sftp` connects to `SFTP_URL`, for example `ssh://backup@example.com:22/backups`, and keeps the repository in that directory. The tool authenticates with either a password or an SSH private key. Secrets can be provided directly with env vars or through Docker secret files. File paths are configurable and default to Docker secret paths under `/run/secrets`.

Repository files are written under a temporary name and renamed once complete, so an interrupted write never looks like a repository file. `cleanup` removes leftover temporary files.

Without `CACHE_DIR`, each run downloads the repository's index and snapshot files again. They are small compared to the data, but a writable `CACHE_DIR` mount saves that transfer.

## Files that change during a backup

Backups run against live volumes, so files can change while they are read. To keep each archived file consistent, a changed file is read from a private copy where possible, and a file that changed while it was read or copied is read again. The snapshot records the size and times of the exact version archived.

The copies are made in `SCRATCH_DIR`, a writable mount such as a named volume. The image ships `/scratch` writable for any user, and a new named volume mounted there inherits that. When `SCRATCH_DIR` isn't usable, this is logged at startup, there are no reflinks, and large files are streamed live.

Copies are unnamed files (`O_TMPFILE`) that disappear once the file has been read, even if the backup crashes, so `SCRATCH_DIR` stays empty. Files are read a few at a time in parallel, so a few copies can exist at once. A file is copied only if `SCRATCH_MIN_FREE` would remain free afterwards. Otherwise it is read live.

### Reflinks

When `SCRATCH_DIR` is on the same filesystem as the volumes and that filesystem has reflinks (btrfs, XFS), each changed file is reflinked there and read from that copy. A reflink shares the file's data instead of copying it, takes about as long as a stat, and is atomic: a concurrent write lands entirely before or entirely after it. The archived file therefore never mixes old and new data, and no retries are needed.

A reflink takes no space until the application rewrites the original, and then at most the file's size. A volume on another filesystem, or on one without reflinks, falls back to the reads below. Set `REFLINK=off` to never reflink.

Reflinks make each file consistent on its own. Files that must match each other, such as a database and its write-ahead log, can still be copied at slightly different moments.

### Retries

Without a reflink, a file is checked for changes: if its size, modification time, or change time differs after the read, the read is discarded, and the file is read again after `RETRY_DELAY`.

- Files up to `RETRY_BUFFER_SIZE` are read into memory. A small file that changes and is then deleted, such as a SQLite journal, never reaches the snapshot. At most four times `RETRY_BUFFER_SIZE` is held in memory; beyond that, small files are copied into `SCRATCH_DIR` instead.
- Larger files changed within `COPY_CHANGED_WITHIN` are fully copied into `SCRATCH_DIR` and the copy is checked the same way.
- Other large files are streamed straight into the snapshot. They haven't changed for a while, so they rarely change during the read. When one does, it is logged as possibly inconsistent and not read again. This avoids copying large files that rarely change, such as media.

A file gets at most `RETRY_ATTEMPTS` reads, and all retries of a volume together stop after `RETRY_VOLUME_BUDGET`. When a limit is hit, the last read is archived and logged as possibly inconsistent.

## Failure handling

Backups run against live volumes and are best-effort:

- A file or directory that is deleted, replaced, or unreadable while the backup runs is skipped with a warning. The rest of the volume is still archived.
- A streamed file is archived at the size it had when opened. A file that grows meanwhile is cut at that size. A file that shrinks or fails to read part-way is padded with zeros, and is logged as possibly inconsistent.
- A snapshot is saved only after all of its data was written, so an interrupted backup leaves no snapshot. The data it already wrote is either reused by the next backup or freed by `cleanup`.
- A volume whose backup fails, for example because of a storage error, is logged, and the remaining volumes are still processed. `restore-all` and `cleanup` also continue past a failing volume.
- After a run, `schedule` applies retention only to volumes that were backed up successfully, so a failing volume keeps all of its older snapshots.

Each backup log line reports how much new data was added, how many files were unchanged, reflinked, and read again, and how many problems were skipped or archived as possibly inconsistent. `backup-all`, `restore-all`, and `cleanup` exit with an error when at least one volume failed. Skipped files alone don't cause an error.

Only one process may write to the repository at a time: don't run `cleanup` from a second container while `schedule` is running.

## Retention and cleanup

`cleanup` removes the snapshots of each volume that the `KEEP_*` options don't keep, then prunes the repository: data no snapshot uses anymore is freed, and packs that are only partly used are rewritten. A pack that becomes unused is deleted by a later `cleanup`, at least 23 hours afterwards, so a backup running meanwhile can't lose data it relies on.

`KEEP_LAST` keeps the newest snapshots. `KEEP_DAILY`, `KEEP_WEEKLY`, and `KEEP_MONTHLY` additionally keep the newest snapshot of each of the last N days, weeks, or months, like `restic forget`.

## Compression

Chunks are compressed with zstd. `COMPRESSION_LEVEL` sets the level when the repository is created, from -7 to 22; rustic's default is 3. Data is split into chunks before it is compressed, so compression doesn't get in the way of deduplication.

## Commands

```text
docker-volume-backups [OPTIONS] <COMMAND>
```

Commands:

```text
backup <VOLUME>                   Back up one volume
restore <VOLUME> [--snapshot ID]  Restore one volume, defaulting to the newest snapshot
backup-all                        Back up every subdirectory under VOLUMES_ROOT
restore-all                       Restore every subdirectory under VOLUMES_ROOT from its newest snapshot
snapshots [VOLUME]                List the snapshots of one or every volume
cleanup                           Apply the retention policy and prune the repository
schedule [--backup-cron X]        Run the cron scheduler
```

If no command is supplied, help is shown.

## Configuration

Every option can be supplied as a CLI flag or environment variable. Command-specific flags are shown with their command.

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--volumes-root` | `VOLUMES_ROOT` | `/volumes` | Directory whose subdirectories are backup volumes. |
| `--storage-driver` | `STORAGE_DRIVER` | `local` | `local` or `sftp`. |
| `--local-storage-root` | `LOCAL_STORAGE_ROOT` | `/backups` | Repository directory for the `local` driver. |
| `--sftp-url` | `SFTP_URL` | unset | SFTP repository directory, such as `ssh://user@host:22/folder`. |
| `--sftp-password` | `SFTP_PASSWORD` | unset | SFTP password secret. |
| `--sftp-password-file` | `SFTP_PASSWORD_FILE` | `/run/secrets/SFTP_PASSWORD` | File containing the SFTP password. |
| `--sftp-private-key` | `SFTP_PRIVATE_KEY` | unset | SSH private key secret. |
| `--sftp-private-key-file` | `SFTP_PRIVATE_KEY_FILE` | `/run/secrets/SFTP_PRIVATE_KEY` | File containing the SSH private key. |
| `--sftp-private-key-passphrase` | `SFTP_PRIVATE_KEY_PASSPHRASE` | unset | SSH private key passphrase secret. |
| `--sftp-private-key-passphrase-file` | `SFTP_PRIVATE_KEY_PASSPHRASE_FILE` | `/run/secrets/SFTP_PRIVATE_KEY_PASSPHRASE` | File containing the key passphrase. |
| `--repository-password` | `REPOSITORY_PASSWORD` | unset | Password that encrypts the repository. Required. |
| `--repository-password-file` | `REPOSITORY_PASSWORD_FILE` | `/run/secrets/REPOSITORY_PASSWORD` | File containing the repository password. |
| `--snapshot-host` | `SNAPSHOT_HOST` | `docker-volume-backups` | Host name recorded in snapshots. Must differ between servers sharing a repository. |
| `--compression-level` | `COMPRESSION_LEVEL` | `3` | zstd level, -7 to 22, of a new repository. See [Compression](#compression). |
| `--cache-dir` | `CACHE_DIR` | unset | Writable directory caching index and snapshot files between runs. |
| `--keep-last` | `KEEP_LAST` | `7` | Newest snapshots to keep per volume. |
| `--keep-daily` | `KEEP_DAILY` | unset | Also keep the newest snapshot of each of the last N days. |
| `--keep-weekly` | `KEEP_WEEKLY` | unset | Also keep the newest snapshot of each of the last N weeks. |
| `--keep-monthly` | `KEEP_MONTHLY` | unset | Also keep the newest snapshot of each of the last N months. |
| `--scratch-dir` | `SCRATCH_DIR` | `/scratch` | Writable directory for temporary copies of files. See [Files that change during a backup](#files-that-change-during-a-backup). |
| `--scratch-min-free` | `SCRATCH_MIN_FREE` | `2GiB` | Free space that must remain in `SCRATCH_DIR` after a copy, or after a reflinked file were fully rewritten. |
| `--reflink` | `REFLINK` | `auto` | `auto` reflinks files into `SCRATCH_DIR` when it is on the volumes' filesystem and that supports reflinks, `off` never does. See [Reflinks](#reflinks). |
| `--retry-attempts` | `RETRY_ATTEMPTS` | `3` | Reads of a file that keeps changing before its last read is archived. `1` disables retries. See [Retries](#retries). |
| `--retry-delay` | `RETRY_DELAY` | `5s` | Wait before reading a changed file again. |
| `--retry-volume-budget` | `RETRY_VOLUME_BUDGET` | `10m` | Time limit for all retries of one volume. |
| `--retry-buffer-size` | `RETRY_BUFFER_SIZE` | `16MiB` | Files up to this size are read into memory, larger ones copied into `SCRATCH_DIR`. Either way a file is archived once a read was stable. |
| `--copy-changed-within` | `COPY_CHANGED_WITHIN` | `1h` | Large files are copied before reading only if they changed within this time. Older ones are streamed live. |
| `schedule --backup-cron` | `BACKUP_CRON` | `0 1 * * *` | Cron expression for `schedule`. Five-field cron expressions are accepted. |

Durations accept values such as `500ms`, `5s`, `10m`, or `1h`. Size values accept `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, and `TiB`.

Logs default to `info`. Set `RUST_LOG` to override the log filter. Backup logs include each new snapshot's ID and how much data it added.

## Docker Compose: local storage

This example backs up two read-only volumes into a repository on a writable local backup volume. The container runs as a non-root user, drops all Linux capabilities, disables privilege escalation, runs with a read-only root filesystem, and applies resource limits.

```yaml
services:
  backup:
    image: ghcr.io/nikarh/docker-volume-backups:latest
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    cpus: "0.50"
    mem_limit: 512m
    environment:
      STORAGE_DRIVER: local
      LOCAL_STORAGE_ROOT: /backups
      BACKUP_CRON: "0 1 * * *"
      KEEP_LAST: 7
      KEEP_WEEKLY: 4
    volumes:
      - app-data:/volumes/app-data:ro
      - media-data:/volumes/media-data:ro
      - backup-data:/backups:rw
      # Temporary copies of files that may change while being read.
      - scratch:/scratch
    secrets:
      - REPOSITORY_PASSWORD

secrets:
  REPOSITORY_PASSWORD:
    file: ./repository-password

volumes:
  app-data:
  media-data:
  backup-data:
  scratch:
```

No additional capabilities are required for local storage; the writable `/backups` mount is enough. A new `scratch` volume is created writable for any user.

## Docker Compose: SFTP storage

This example backs up to SFTP using an SSH private key mounted as a Docker secret, and caches the repository index between runs.

```yaml
services:
  backup:
    image: ghcr.io/nikarh/docker-volume-backups:latest
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    cpus: "0.50"
    mem_limit: 512m
    environment:
      STORAGE_DRIVER: sftp
      SFTP_URL: ssh://backup@example.com:22/server-a
      SFTP_PRIVATE_KEY_FILE: /run/secrets/SFTP_PRIVATE_KEY
      SNAPSHOT_HOST: server-a
      CACHE_DIR: /cache
      BACKUP_CRON: "0 1 * * *"
      KEEP_DAILY: 7
      KEEP_WEEKLY: 4
      KEEP_MONTHLY: 6
    volumes:
      - app-data:/volumes/app-data:ro
      - media-data:/volumes/media-data:ro
      # Temporary copies of files that may change while being read.
      - scratch:/scratch
      - cache:/cache
    secrets:
      - SFTP_PRIVATE_KEY
      - REPOSITORY_PASSWORD

secrets:
  SFTP_PRIVATE_KEY:
    file: ./id_ed25519
  REPOSITORY_PASSWORD:
    file: ./repository-password

volumes:
  app-data:
  media-data:
  scratch:
  cache:
```

The image ships `/cache` writable for any user, like `/scratch`, so a new named volume mounted there is writable too.

## Restore

Run restore as a one-off container. The destination volume must be writable for restore. Files are restored with their numeric owners, which needs the `CHOWN` and `FOWNER` capabilities. Files in the volume that aren't in the snapshot are left in place. Use `restore-all` to restore every mounted volume under `VOLUMES_ROOT`, and `snapshots` to list the snapshots to restore from.

```sh
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --cap-add CHOWN \
  --cap-add FOWNER \
  --cap-add DAC_OVERRIDE \
  --security-opt no-new-privileges:true \
  -e STORAGE_DRIVER=local \
  -e LOCAL_STORAGE_ROOT=/backups \
  -e REPOSITORY_PASSWORD_FILE=/repository-password \
  -v ./repository-password:/repository-password:ro \
  -v app-data:/volumes/app-data:rw \
  -v backup-data:/backups:ro \
  ghcr.io/nikarh/docker-volume-backups:latest \
  restore app-data
```

```sh
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --cap-add CHOWN \
  --cap-add FOWNER \
  --cap-add DAC_OVERRIDE \
  --security-opt no-new-privileges:true \
  -e STORAGE_DRIVER=local \
  -e LOCAL_STORAGE_ROOT=/backups \
  -e REPOSITORY_PASSWORD_FILE=/repository-password \
  -v ./repository-password:/repository-password:ro \
  -v app-data:/volumes/app-data:rw \
  -v media-data:/volumes/media-data:rw \
  -v backup-data:/backups:ro \
  ghcr.io/nikarh/docker-volume-backups:latest \
  restore-all
```

The repository can also be read without this tool, for example with `restic -r /path/to/repository snapshots` or `restic restore <id> --target ...`.

## License

This project is licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
