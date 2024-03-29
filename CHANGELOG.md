# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.9.4] - 2024-03-29

### Changed

* Use reduct-base 1.9.4

## [1.9.3] - 2024-03-22

### Fixed

* Fix sorting batched data, [PR-5](https://github.com/reductstore/reduct-rs/pull/5)
* Add Send and Sync traits to Record.bytes(), [PR-6](https://github.com/reductstore/reduct-rs/pull/6)

## [1.9.2] - 2024-03-08

### Fixed

* Use rustls 0.21.0

## [1.9.0] - 2024-03-08

### Added

* RS-177: Add license field to ServerInfo, [PR-1](https://github.com/reductstore/reduct-rs/pull/1)
* RS-185: Add `verify_ssl` option¸ [PR-2](https://github.com/reductstore/reduct-rs/pull/2)
* `Client.get_token` method, [PR-3](

## [1.8.0] - 2024-01-24

* Part of https://github.com/reductstore/reductstore

[Unreleased]: https://github.com/reductstore/reduct-rs/compare/v1.9.3...HEAD

[1.9.3]: https://github.com/reductstore/reduct-rs/compare/v1.9.2...v1.9.3

[1.9.2]: https://github.com/reductstore/reduct-rs/compare/v1.9.0...v1.9.2

[1.9.0]: https://github.com/reductstore/reduct-rs/compare/v1.8.0...v1.9.0

[1.8.0]: https://github.com/reductstore/reduct-rs/compare/tag/v1.8.0
