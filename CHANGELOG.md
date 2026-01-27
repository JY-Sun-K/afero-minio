# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Complete implementation of all afero.Fs interface methods
- Production-ready error handling with custom error types
- Context cancellation support throughout the codebase
- O_TRUNC flag support in OpenFile
- Comprehensive input validation
- Enhanced DSN parsing with validation
- PathError type for better error reporting
- Extensive unit tests covering all major functionality
- golangci-lint configuration for code quality

### Fixed
- Fixed ByName.Swap implementation to properly swap FileInfo objects
- Fixed dangerous log.Fatalln usage in RemoveAll - now returns errors properly
- Fixed file permission checks in Read/Write operations
- Fixed Seek implementation with proper error handling
- Fixed resource management in file_source.go
- Fixed directory detection logic in newFileInfoFromAttrs
- Fixed memory leaks in reader/writer management
- Corrected typos ("opend" -> "opened")
- Fixed negative offset handling across all operations
- Fixed context cancellation checks

### Changed
- Improved WriteAt to handle non-zero offsets correctly
- Enhanced Truncate to handle non-existent files
- Better validation in ParseURL with detailed error messages
- Improved Stat to handle virtual directories
- Enhanced Mkdir/MkdirAll with proper existence checks
- Optimized Rename with atomic-like behavior (copy + delete with cleanup)
- Updated error messages to be more descriptive

### Security
- Added input validation to prevent negative offsets
- Added context timeout support
- Improved error handling to prevent information leakage
- Added boundary condition checks throughout

## [1.0.0] - 2024-01-XX

### Added
- Initial implementation of MinIO filesystem for Afero
- Basic file operations (Create, Open, Read, Write)
- Directory operations (Mkdir, MkdirAll, Readdir)
- File manipulation (Remove, RemoveAll, Rename, Stat)
- DSN-based configuration

[Unreleased]: https://github.com/cpyun/afero-minio/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/cpyun/afero-minio/releases/tag/v1.0.0
