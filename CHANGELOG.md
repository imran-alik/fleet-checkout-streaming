# Changelog

All notable changes to this portfolio project are documented here.

## [1.0.0] — 2026-09-14

### Added

- Initial fleet rental CDC streaming pipeline (local emulator)
- Modular package `codebase/fleet_streaming/` with medallion DuckDB loader
- In-process Kafka queue + Redis inventory TTL emulator
- Committed 200-event JSONL sample and certification script
- 7 pytest scenarios + design-commit static gates
- Full documentation set (DESIGN, HANDOVER, SOLUTION, GCP mapping)
