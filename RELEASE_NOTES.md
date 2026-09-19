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

