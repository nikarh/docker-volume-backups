# fileserver-backup

`fileserver-backup` is a small Rust backup tool for Dockerized file servers. It discovers volumes mounted as subdirectories, streams each volume into a `.tar.xz` archive, stores archives in either local storage or SFTP storage, restores a single volume, and removes old archives using a retention policy.

The container defaults to `fileserver-backup run`, which waits for `BACKUP_CRON` and then runs `backup-all` followed by `cleanup`. The default schedule is daily at 01:00 local time:

```cron
0 1 * * *
```

## Storage

Storage is selected with `STORAGE_DRIVER`.

`local` writes archives to `LOCAL_STORAGE_ROOT/<volume>/<timestamp>.tar.xz`. This is useful when a writable backup disk is mounted into the container.

`sftp` connects to `SFTP_URL`, for example `ssh://backup@example.com:22/backups`. The tool authenticates with either a password or an SSH private key. Secrets can be provided directly with env vars or through Docker secret files. File paths are configurable and default to Docker secret paths under `/run/secrets`.

SFTP backups stream the tar.xz encoder directly into the remote SFTP file. The archive is not created on local disk first.

## Commands

```text
fileserver-backup [OPTIONS] [COMMAND]
```

Commands:

```text
backup <VOLUME>                 Back up one volume
restore <VOLUME> [--archive X]  Restore one volume, defaulting to the newest archive
backup-all                      Back up every subdirectory under VOLUMES_ROOT
cleanup                         Apply retention policy to every discovered volume
run                             Run the cron scheduler
```

If no command is supplied, `run` is used.

## Configuration

Every option can be supplied as a CLI flag or environment variable.

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
| `--retention-policy` | `RETENTION_POLICY` | `count` | `count` or `size`. |
| `--retention-count` | `RETENTION_COUNT` | `7` | Number of archives to keep per volume for `count` policy. |
| `--retention-min-count` | `RETENTION_MIN_COUNT` | `2` | Minimum archives to keep per volume for `size` policy. |
| `--retention-max-total-size` | `RETENTION_MAX_TOTAL_SIZE` | `10GiB` | Maximum total archive size per volume for `size` policy. |
| `--backup-cron` | `BACKUP_CRON` | `0 1 * * *` | Cron expression for `run`. Five-field cron expressions are accepted. |

Size values accept `B`, `KB`, `MB`, `GB`, `TB`, `KiB`, `MiB`, `GiB`, and `TiB`.

## Docker Compose: local storage

This example backs up two read-only volumes into a writable local backup volume. The container runs as a non-root user, drops all Linux capabilities, disables privilege escalation, runs with a read-only root filesystem, and applies resource limits.

```yaml
services:
  backup:
    image: ghcr.io/nikarh/fileserver-backup:latest
    user: "65532:65532"
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    pids_limit: 128
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

volumes:
  app-data:
  media-data:
  backup-data:
```

No additional capabilities are required for local storage; the writable `/backups` mount is enough.

## Docker Compose: SFTP storage

This example streams backups to SFTP using an SSH private key mounted as a Docker secret.

```yaml
services:
  backup:
    image: ghcr.io/nikarh/fileserver-backup:latest
    user: "65532:65532"
    read_only: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    pids_limit: 128
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
    secrets:
      - SFTP_PRIVATE_KEY

secrets:
  SFTP_PRIVATE_KEY:
    file: ./id_ed25519

volumes:
  app-data:
  media-data:
```

## Restore

Run restore as a one-off container. The destination volume must be writable for restore.

```sh
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --user 65532:65532 \
  -e STORAGE_DRIVER=local \
  -e LOCAL_STORAGE_ROOT=/backups \
  -v app-data:/volumes/app-data:rw \
  -v backup-data:/backups:ro \
  ghcr.io/nikarh/fileserver-backup:latest \
  restore app-data
```
