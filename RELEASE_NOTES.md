# roo_monitoring 1.2.0

- Added `Collection::compactedUntil()`, which returns the exclusive data timestamp through which compaction has succeeded at every resolution, or `-1` before the first successful compaction.
- Historical vault compaction and log cleanup now resume after a restart. Source logs are kept until compaction finishes at every resolution.
- Compaction now stops on log or vault I/O errors, including full disks and truncated records, and preserves source logs for retry with a new writer. Fixed checkpointing when a short vault ends cleanly between records.
- Fixed Arduino dependency discovery for `roo_io` and `roo_collections`.
- Updated `roo_collections` to 2.0.2, `roo_io` to 2.5.0, and `roo_logging` to 1.6.1, with matching PlatformIO minimum versions.
- Updated Bazel and CI to `roo_testing` 2.7.0. Added ESP32-S3, ESP32-C3, and RP2040 host-emulation profile definitions and expanded the test helper to select nine profiles. Running the full matrix locally requires additional `.bazelrc` imports.

---

# roo_monitoring 1.1.5

- Fixed vault reads to reject truncated or malformed sample counts instead of treating them as empty entries; added regression tests.
- Updated dependencies to `roo_collections` 1.4.8, `roo_io` 2.4.0, and `roo_logging` 1.5.11, including matching PlatformIO minimum versions.
- Upgraded `roo_testing` to 2.3.0.
- Added an ESP-IDF ESP32 test profile, automatic profile selection for ESP-IDF example runs, and a helper to test both Arduino and ESP-IDF profiles.

---
# roo_monitoring 1.1.4

- Updated Roo dependencies to roo_collections 1.4.7, roo_io 2.3.0, and roo_logging 1.5.10, including matching PlatformIO minimum versions.
- Updated Bazel dependencies to rules_cc 0.2.25, googletest 1.18.0.bcr.1, and roo_testing 2.1.2.
- Updated the shared CI workflow to roo_testing 2.1.2.
- Added consolidated release notes for previous releases.

---

# [roo_monitoring 1.1.3](https://github.com/dejwk/roo_monitoring/releases/tag/1.1.3)

Published 2026-08-30.

This release modernizes the project’s host-build and CI tooling, while updating its Roo dependencies.

### Changed

- Adopted roo_testing 2.x Arduino ESP32 host-emulation profiles.
- Simplified local Bazel usage: standard commands now default to the Arduino profile when using Bazelisk.
- Centralized AddressSanitizer configuration.
- Modernized GitHub Actions CI with pull-request and manual-run support.
- Updated dependencies:
  - roo_testing 2.1.0
  - roo_collections 1.4.6
  - roo_io 2.2.7
  - roo_logging 1.5.8

### Documentation

- Added host-emulation build and test instructions to the README.

No library API changes are included in this release.

**Full Changelog:** https://github.com/dejwk/roo_monitoring/compare/1.1.2...1.1.3

---

# [roo_monitoring 1.1.2](https://github.com/dejwk/roo_monitoring/releases/tag/1.1.2)

Published 2026-06-04.

Added documentation.


---

# [roo_monitoring 1.1.1](https://github.com/dejwk/roo_monitoring/releases/tag/1.1.1)

Published 2026-02-26.

Fixed a memory leak caused by a bug in roo_io.

---

# [roo_monitoring 1.1.0](https://github.com/dejwk/roo_monitoring/releases/tag/1.1.0)

Published 2026-02-26.

* Several bugs fixed.
* Added unit tests.
* Updated dependencies.
* Compiles without warnings.
* Added doxygen documentation.

**Full Changelog**: https://github.com/dejwk/roo_monitoring/compare/1.0.0...1.1.0

---

# [roo_monitoring 1.0.0](https://github.com/dejwk/roo_monitoring/releases/tag/1.0.0)

Published 2026-02-08.

Initial release.

---

