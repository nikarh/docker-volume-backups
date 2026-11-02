# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [0.3.0] - 2026-11-02
### Details

#### Added
- Add project license files by @nikarh

#### Changed
- Make backup execution explicit by @nikarh
- Log backup archive sizes by @nikarh
- Zstd compression by default and buffered storage I/O (#1) by @nikarh in #1
- V0.3.0

#### Fixed
- Cover storage directory creation by @nikarh
- Keep backing up when files change or a volume fails (#2) by @nikarh in #2
[0.3.0]: https://github.com/nikarh/docker-volume-backups/compare/v0.2.0..v0.3.0

