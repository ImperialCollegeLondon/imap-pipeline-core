# Claude Notes

- you must use Poetry for dependency management and virtual environments. If dependencies are not available, run `poetry install` to set them up.
- you must never use pip directly to install dependencies. Always use Poetry.
- activate the virtual env by running `source .venv/bin/activate` from the project root, or run commands under poeetry using `poetry run <command>`
- run `pre-commit install` to set up pre-commit hooks for code formatting and linting
- you must always run `pre-commit run --all-files` before claiming to be finished and before committing code to ensure formatting and linting checks pass

## Running Tests

- test files are located in the `tests/` directory and files are named with the `test_*.py` pattern with one file per file under test.
- Use `poetry run pytest` to run tests in the virtual environment
