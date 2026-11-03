# docker-volume-backups

`docker-volume-backups` is a small Rust backup tool for Dockerized file servers. It discovers volumes mounted as subdirectories, streams each volume into a `.tar.zst` (or `.tar.xz`) archive, stores archives in either local storage or SFTP storage, restores a single volume, and removes old archives using a retention policy.

The container defaults to `docker-volume-backups schedule`, which waits for `BACKUP_CRON` and then runs `backup-all` followed by `cleanup`. The default schedule is daily at 01:00 local time:

```cron
0 1 * * *
```

## Storage

Storage is selected with `STORAGE_DRIVER`.

`local` writes archives to `LOCAL_STORAGE_ROOT/<volume>/<timestamp>.tar.zst`. This is useful when a writable backup disk is mounted into the container.

`sftp` connects to `SFTP_URL`, for example `ssh://backup@example.com:22/backups`. The tool authenticates with either a password or an SSH private key. Secrets can be provided directly with env vars or through Docker secret files. File paths are configurable and default to Docker secret paths under `/run/secrets`.

SFTP backups stream the compressed tar directly into the remote SFTP file. The archive is not created on local disk first.

Both storage drivers create missing destination directories recursively when writing archives, including per-volume directories.

## Files that change during a backup

Backups run against live volumes, so files can change while they are read. To keep each archived file consistent, files are read from a private copy where possible, and a file that changed while it was read or copied is read again. Every file ends up in the archive exactly once.

The copies are made in `SCRATCH_DIR`, a writable mount such as a named volume. The image ships `/scratch` writable for any user, and a new named volume mounted there inherits that. When `SCRATCH_DIR` isn't usable, this is logged at startup, there are no reflinks, and large files are streamed live.

Copies are unnamed files (`O_TMPFILE`) that disappear once the file has been read, even if the backup crashes, so `SCRATCH_DIR` stays empty. At most one copy exists at a time, and a file is copied only if `SCRATCH_MIN_FREE` would remain free afterwards. Otherwise it is read live.

### Reflinks

When `SCRATCH_DIR` is on the same filesystem as the volumes and that filesystem has reflinks (btrfs, XFS), each file is reflinked there and the archive is read from that copy. A reflink shares the file's data instead of copying it, takes about as long as a stat, and is atomic: a concurrent write lands entirely before or entirely after it. The archived file therefore never mixes old and new data, and no retries are needed.

A reflink takes no space until the application rewrites the original, and then at most the file's size. A volume on another filesystem, or on one without reflinks, falls back to the reads below. Set `REFLINK=off` to never reflink.

Reflinks make each file consistent on its own. Files that must match each other, such as a database and its write-ahead log, can still be copied at slightly different moments.

### Retries

Without a reflink, a file is checked for changes: if its size, modification time, or change time differs after the read, the read is discarded and the file is read again after the rest of the volume.

- Files up to `RETRY_BUFFER_SIZE` are read into memory. A small file that changes and is then deleted, such as a SQLite journal, never reaches the archive.
- Larger files changed within `COPY_CHANGED_WITHIN` are fully copied into `SCRATCH_DIR` and the copy is checked the same way.
- Other large files are streamed straight into the archive. They haven't changed for a while, so they rarely change during the read. When one does, its bytes are already in the archive and can't be taken back, so it is logged as possibly inconsistent and not read again. This avoids rewriting large files that rarely change, such as media, on every backup.

Retries run in passes, `RETRY_DELAY` apart, until every file was read without changing. A file gets at most `RETRY_ATTEMPTS` reads, and all retries of a volume together stop after `RETRY_VOLUME_BUDGET`. When a limit is hit, the last read is archived and logged as possibly inconsistent.

## Failure handling

Backups run against live volumes and are best-effort:

- A file or directory that is deleted, replaced, or unreadable while the backup runs is skipped with a warning. The rest of the volume is still archived.
- Each streamed file is archived at the size it had when opened. A file that grows meanwhile is cut at that size. A file that shrinks or fails to read part-way is padded with zeros, so the archive stays valid, and is logged as possibly inconsistent.
- Archives are written as `<timestamp>.tar.zst.partial` and renamed only after the whole stream has been written. An interrupted backup never appears as the latest archive for restore or retention, and `cleanup` removes leftover partial files.
- A volume whose backup fails, for example because of a storage error, is logged, and the remaining volumes are still processed. `restore-all` and `cleanup` also continue past a failing volume.
- After a run, `schedule` applies retention only to volumes that were backed up successfully, so a failing volume keeps all of its older archives.

Each backup log line reports how many files were archived, reflinked, and read again, and how many problems were skipped or archived as possibly inconsistent. `backup-all`, `restore-all`, and `cleanup` exit with an error when at least one volume failed. Skipped files alone don't cause an error.

## Compression

New archives are compressed with zstd level 3 by default (`.tar.zst`). Set `COMPRESSION=xz` for `.tar.xz` archives and `COMPRESSION_LEVEL` to tune either engine.

zstd is the better fit for small servers. On an Intel J5040, zstd level 3 compressed a 200 MB database in about 3 seconds, while xz level 6 took about 150 seconds (roughly 1.3 MB/s) for an archive about half the size. xz makes sense only when storage is much scarcer than CPU time.

Restores choose the decoder from the archive extension, so switching engines keeps older archives restorable. Retention treats both formats as one timeline ordered by timestamp, so the old format's archives age out normally.

Source files are read in 2 MiB blocks, and archive data is written to and read from storage through 4 MiB buffers. Over SFTP, this keeps many packets in flight instead of waiting for an acknowledgement after every few kilobytes. With 1 ms of latency each way, this made a 200 MB SFTP transfer about 25x faster.

## Commands

```text
docker-volume-backups [OPTIONS] <COMMAND>
```

Commands:

```text
backup <VOLUME>                 Back up one volume
restore <VOLUME> [--archive X]  Restore one volume, defaulting to the newest archive
backup-all                      Back up every subdirectory under VOLUMES_ROOT
restore-all [--archive X]       Restore every subdirectory under VOLUMES_ROOT
cleanup                         Apply retention policy to every discovered volume
schedule [--backup-cron X]      Run the cron scheduler
```

If no command is supplied, help is shown.

## Configuration

Every option can be supplied as a CLI flag or environment variable. Command-specific flags are shown with their command.

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--volumes-root` | `VOLUMES_ROOT` | `/volumes` | Directory whose subdirectories are backup volumes. |
| `--storage-driver` | `STORAGE_DRIVER` | `local` | `local` or `sftp`. |
| `--local-storage-root` | `LOCAL_STORAGE_ROOT` | `/backups` | Local archive root for the `local` driver. |
| `--sftp-url` | `SFTP_URL` | unset | SFTP destination, such as `ssh://user@host:22/folder`. |
| `--sftp-password` | `SFTP_PASSWORD` | unset | SFTP password secret. |
| `--sftp-password-file` | `SFTP_PASSWORD_FILE` | `/run/secrets/SFTP_PASSWORD` | File containing the SFTP password. |
| `--sftp-private-key` | `SFTP_PRIVATE_KEY` | unset | SSH private key secret. |
| `--sftp-private-key-file` | `SFTP_PRIVATE_KEY_FILE` | `/run/secrets/SFTP_PRIVATE_KEY` | File containing the SSH private key. |
| `--sftp-private-key-passphrase` | `SFTP_PRIVATE_KEY_PASSPHRASE` | unset | SSH private key passphrase secret. |
| `--sftp-private-key-passphrase-file` | `SFTP_PRIVATE_KEY_PASSPHRASE_FILE` | `/run/secrets/SFTP_PRIVATE_KEY_PASSPHRASE` | File containing the key passphrase. |
| `--compression` | `COMPRESSION` | `zstd` | `zstd` or `xz` for new archives. Restores pick the decoder from the archive extension, so existing `.tar.xz` archives stay restorable and count toward retention. See [Compression](#compression). |
| `--compression-level` | `COMPRESSION_LEVEL` | `3` (zstd), `6` (xz) | Compression level: 1-22 for zstd, 0-9 for xz. |
| `--retention-policy` | `RETENTION_POLICY` | `count` | `count` or `size`. |
| `--retention-count` | `RETENTION_COUNT` | `7` | Number of archives to keep per volume for `count` policy. |
| `--retention-min-count` | `RETENTION_MIN_COUNT` | `2` | Minimum archives to keep per volume for `size` policy. |
| `--retention-max-total-size` | `RETENTION_MAX_TOTAL_SIZE` | `10GiB` | Maximum total archive size per volume for `size` policy. |
| `--scratch-dir` | `SCRATCH_DIR` | `/scratch` | Writable directory for temporary copies of files. See [Files that change during a backup](#files-that-change-during-a-backup). |
| `--scratch-min-free` | `SCRATCH_MIN_FREE` | `2GiB` | Free space that must remain in `SCRATCH_DIR` after a copy, or after a reflinked file were fully rewritten. |
| `--reflink` | `REFLINK` | `auto` | `auto` reflinks files into `SCRATCH_DIR` when it is on the volumes' filesystem and that supports reflinks, `off` never does. See [Reflinks](#reflinks). |
| `--retry-attempts` | `RETRY_ATTEMPTS` | `3` | Reads of a file that keeps changing before its last copy is archived. `1` disables retries. See [Retries](#retries). |
| `--retry-delay` | `RETRY_DELAY` | `5s` | Wait before each retry pass. |
| `--retry-volume-budget` | `RETRY_VOLUME_BUDGET` | `10m` | Time limit for all retries of one volume. |
| `--retry-buffer-size` | `RETRY_BUFFER_SIZE` | `16MiB` | Files up to this size are read into memory, larger ones copied into `SCRATCH_DIR`. Either way a file is archived once a read was stable. |
| `--copy-changed-within` | `COPY_CHANGED_WITHIN` | `1h` | Large files are copied before reading only if they changed within this time. Older ones are streamed live. |
| `schedule --backup-cron` | `BACKUP_CRON` | `0 1 * * *` | Cron expression for `schedule`. Five-field cron expressions are accepted. |

Durations accept values such as `500ms`, `5s`, `10m`, or `1h`. Size values accept `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, and `TiB`.

Logs default to `info`. Set `RUST_LOG` to override the log filter. Backup logs include each completed archive name and compressed size.

## Docker Compose: local storage

This example backs up two read-only volumes into a writable local backup volume. The container runs as a non-root user, drops all Linux capabilities, disables privilege escalation, runs with a read-only root filesystem, and applies resource limits.

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
      RETENTION_POLICY: count
      RETENTION_COUNT: 7
    volumes:
      - app-data:/volumes/app-data:ro
      - media-data:/volumes/media-data:ro
      - backup-data:/backups:rw
      # Temporary copies of files that may change while being read.
      - scratch:/scratch

volumes:
  app-data:
  media-data:
  backup-data:
  scratch:
```

No additional capabilities are required for local storage; the writable `/backups` mount is enough. A new `scratch` volume is created writable for any user.

## Docker Compose: SFTP storage

This example streams backups to SFTP using an SSH private key mounted as a Docker secret.

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
      BACKUP_CRON: "0 1 * * *"
      RETENTION_POLICY: size
      RETENTION_MIN_COUNT: 3
      RETENTION_MAX_TOTAL_SIZE: 50GiB
    volumes:
      - app-data:/volumes/app-data:ro
      - media-data:/volumes/media-data:ro
      # Temporary copies of files that may change while being read.
      - scratch:/scratch
    secrets:
      - SFTP_PRIVATE_KEY

secrets:
  SFTP_PRIVATE_KEY:
    file: ./id_ed25519

volumes:
  app-data:
  media-data:
  scratch:
```

## Restore

Run restore as a one-off container. The destination volume must be writable for restore. Use `restore-all` to restore every mounted volume under `VOLUMES_ROOT`.

```sh
# The cap-add entries are only needed for restore when preserving ownership and metadata.
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --cap-add CHOWN \
  --cap-add FOWNER \
  --cap-add DAC_OVERRIDE \
  --security-opt no-new-privileges:true \
  -e STORAGE_DRIVER=local \
  -e LOCAL_STORAGE_ROOT=/backups \
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
  -v app-data:/volumes/app-data:rw \
  -v media-data:/volumes/media-data:rw \
  -v backup-data:/backups:ro \
  ghcr.io/nikarh/docker-volume-backups:latest \
  restore-all
```

## License

This project is licensed under either of:

- [MIT License](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)
