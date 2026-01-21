# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

IMAP Pipeline Core is a Python data processing pipeline for the IMAP (Interstellar Mapping and Acceleration Probe) mission. It processes magnetometer data from multiple sources (WebPODA, IMAP SDC CDF files API, I-ALiRT API), performs calibration, and manages a  shared folder of file saved in a data store. The data-store data is tracked in a postgres database which also contains some packet data mirrored from CSV and CDF files.

The project uses Prefect for workflow orchestration, Typer for CLI commands, SQLAlchemy ORM with Alembic for database management, and Pydantic for configuration management. It includes a magnetometer calibration toolkit and supports automated testing with pytest.

## Build & Development Commands

```bash
# Setup
poetry install                          # Install dependencies (never use pip directly)
source .venv/bin/activate               # Activate virtual environment
pre-commit install                      # Set up git hooks

# Running commands
poetry run imap-mag <command>           # Run main CLI
poetry run imap-db <command>            # Run database CLI

# Testing
poetry run pytest                       # Run all tests
poetry run pytest tests/test_foo.py    # Run single test file
poetry run pytest -k "test_name"        # Run tests matching pattern
poetry run pytest -n auto               # Run tests in parallel

# Code quality (MUST run before committing)
poetry run pre-commit run --all-files   # Run all linting/formatting checks
poetry run ruff check --fix             # Auto-fix linting issues
poetry run ruff format                  # Format code

# Build & package
./build.sh                              # Run linting + tests with coverage
./pack.sh                               # Create distributable packages
./build-docker.sh                       # Build Docker image
```

## Architecture

### Source Modules (`src/`)

- **imap_mag/** - Main magnetometer processing module
  - `cli/` - Typer CLI commands (`fetch/`, `process.py`, `calibrate.py`, `publish.py`, `check/`, `plot/`)
  - `download/` - Data fetchers (FetchBinary, FetchScience, FetchIALiRT)
  - `process/` - Data processing logic
  - `io/` - File I/O and path handling
  - `db/` - Database operations
  - `config/` - Pydantic settings and configuration
  - `util/` - Utilities (TimeConversion, ReferenceFrame, MAGSensor)

- **imap_db/** - Database management with SQLAlchemy ORM and Alembic migrations
  - `model.py` - File table with versioning and soft deletes
  - `migrations/` - Alembic migration scripts

- **mag_toolkit/** - Magnetometer calibration toolkit
  - `calibration/calibrators/` - Calibrator implementations
  - `MatlabWrapper.py` - MATLAB integration for calibration

- **prefect_server/** - Prefect workflow orchestration
  - `workflow.py` - Flow deployment and scheduling
  - `poll*.py` - Data polling flows (HK, Science, I-ALiRT)
  - `postgresUploadFlow.py` - Database synchronization
  - `datastoreCleanupFlow.py` - Old / superseeded file cleanup

CLI commands are documented in the README.md in the project root.

### Key Patterns

- **CLI-first design**: All core logic exposed via Typer CLI commands and mirrored in Prefect workflows
- Data is downloaded as level 0, processed to level 1 or 2. Files are processed in a local work file, then published to the data store, and indexed in the database.
- Prefect flows run in a docker container with environment variables for config
- **File versioning**: Database tracks file versions with soft deletes (`deletion_date`)
- **Configuration**: Pydantic Settings for environment-based config
- **Crump**: Data is extracted from CSV files, processed, and stored in a Postgres database using the python library Crump - docs are at <https://alastairtree.github.io/crump/>

## Testing

- Tests in `tests/` directory, one `test_*.py` file per source file
- Test utilities in `tests/util/` (database fixtures, WireMock, Prefect helpers)
- Test data in `tests/datastore/`
- Expect code to be well covered with automated tests and tests should pass before committing

## Code Style

- Ruff for linting (E, F, I, UP, RUF rules) and formatting
- Google-style docstrings
- Double quotes, 4-space indentation
- Pre-commit hooks enforce style and run tests before commits
- Do not use `pip` directly; always use `poetry` for dependency management
- Always ensure linting and pre-commit checks pass before committing code
- NEVER EVER mention agents like ChatGPT or Claude or copilot in commit messages or code comments.
