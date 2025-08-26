# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


### Fixed

- Fix error handling for batched records in `QueryBuilder.send()`, [PR-44](https://github.com/reductstore/reduct-rs/pull/44)

## [1.16.0] - 2025-07-31

### Breaking Changes

- Drop support for legacy code and ReductStore API versions below 1.13

### Added

- Check server API version and warn if it's too old, [PR-34](https://github.com/reductstore/reduct-rs/pull/34)

## Deprecated

- `each_n`, `each_s` and `limit` parameters in `QueryBuilder` and `ReplicationSettings` are deprecated, use conditional operators instead,  [PR-34](https://github.com/reductstore/reduct-rs/pull/34)

## Removed

- Remove legacy code in remove query, [PR-35](https://github.com/reductstore/reduct-rs/pull/35)
- Deprecated `include\exclude` parameters in `QueryBuilder`, `RemoveQueryBuilder` and `ReplicationSettings`, [PR-41](https://github.com/reductstore/reduct-rs/pull/41)

## [1.15.2] - 2025-06-11

### Fixed

- Fix parsing url with prefix, [PR-37](https://github.com/reductstore/reduct-rs/pull/37)

## [1.15.1] - 2025-05-27

### Changed

- Minimal Rust version is 1.85, [PR-36](https://github.com/reductstore/reduct-rs/pull/36)

## [1.15.0] - 2025-05-07

### Added

- RS-628: Support `ext` parameter to `QueryBuilder`, [PR-32](https://github.com/reductstore/reduct-rs/pull/32)

## [1.14.0] - 2025-02-25

### Added

- RS-550: Add support for when condition on replication settings, [PR-29](https://github.com/reductstore/reduct-rs/pull/29)

## [1.13.0] - 2024-12-04

### Added

- RS-543: Support conditional query, [PR-28](https://github.com/reductstore/reduct-rs/pull/28)

## [1.12.1] - 2024-10-21

### Fixed

- Make `RemoveQueryBuilder` public, [PR-27](https://github.com/reductstore/reduct-rs/pull/27)

## [1.12.0] - 2024-10-04

### Added

- RS-418: Methods `Bucket.remove_record`, `Bucket.remove_batch` and `Bucker.remove_query` for deleting records, [PR-19](https://github.com/reductstore/reduct-rs/pull/19)
- RS-388: Add `Bucket.rename_entry` method, [PR-24](https://github.com/reductstore/reduct-rs/pull/24)
- RS-419: Add `Bucket.rename` method. [PR-25](https://github.com/reductstore/reduct-rs/pull/25)
- RS-462: Improve batching, [PR-26](https://github.com/reductstore/reduct-rs/pull/26)
- Minimal Rust version is 1.80

### Changed

- RS-418: Use builder in `Bucket.remove_record` call, [PR-20](https://github.com/reductstore/reduct-rs/pull/20)

## [1.11.0] - 2024-08-19

### Added

- RS-31: `Bucket.update_record` and `Bucket.update_batch` to change labels of existing records, [PR-16](https://github.com/reductstore/reduct-rs/pull/16)

### Changed

- Unify string arguments in public API, [PR-17](https://github.com/reductstore/reduct-rs/pull/17)

## [1.10.1] - 2024-06-11

### Fixed

- Use `ring` provider, [PR-14](https://github.com/reductstore/reduct-rs/pull/14)

## [1.10.0] - 2024-06-11

### Added

- RS-261: add `each_n` and `each_s` query parameters, [PR-11](https://github.com/reductstore/reduct-rs/pull/11)
- RS-311: add `each_n` and `each_s` replication settings, [PR-13](https://github.com/reductstore/reduct-rs/pull/13)

### Changed

- Use IntoBody and `IntoBytes to write data, [PR-10](https://github.com/reductstore/reduct-rs/pull/10)

## [1.9.5] - 2024-03-30

### Fixed

* Re-export replication structures, [PR-7](https://github.com/reductstore/reduct-rs/pull/7)

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

[Unreleased]: https://github.com/reductstore/reduct-rs/compare/v1.16.0...HEAD

[1.16.0]: https://github.com/reductstore/reduct-rs/compare/v1.15.2...v1.16.0

[1.15.2]: https://github.com/reductstore/reduct-rs/compare/v1.15.1...v1.15.2

[1.15.1]: https://github.com/reductstore/reduct-rs/compare/v1.15.0...v1.15.1

[1.15.0]: https://github.com/reductstore/reduct-rs/compare/v1.14.0...v1.15.0

[1.14.0]: https://github.com/reductstore/reduct-rs/compare/v1.13.0...v1.14.0

[1.13.0]: https://github.com/reductstore/reduct-rs/compare/v1.12.1...v1.13.0

[1.12.1]: https://github.com/reductstore/reduct-rs/compare/v1.12.0...v1.12.1

[1.12.0]: https://github.com/reductstore/reduct-rs/compare/v1.11.0...v1.12.0

[1.11.0]: https://github.com/reductstore/reduct-rs/compare/v1.10.1...v1.11.0

[1.10.1]: https://github.com/reductstore/reduct-rs/compare/v1.10.0...v1.10.1

[1.10.0]: https://github.com/reductstore/reduct-rs/compare/v1.9.2...v1.10.0

[1.9.3]: https://github.com/reductstore/reduct-rs/compare/v1.9.2...v1.9.3

[1.9.2]: https://github.com/reductstore/reduct-rs/compare/v1.9.0...v1.9.2

[1.9.0]: https://github.com/reductstore/reduct-rs/compare/v1.8.0...v1.9.0

[1.8.0]: https://github.com/reductstore/reduct-rs/compare/tag/v1.8.0
