# AGENTS.md - Development Guide for djvu-viewer

## Project Overview

djvu-viewer is a Python project for viewing and converting DjVu files, with integration for MediaWiki-based genealogy archives. It uses FastAPI for the web interface and provides CLI tools for batch processing.

## Build/Lint/Test Commands

### Installation
```bash
scripts/install
```

### Testing
```bash
# Run all tests with unittest discover (default)
scripts/test

# Run tests with green (colorful output)
scripts/test -g
scripts/test --green

# Run tests module-by-module
scripts/test -m
scripts/test --module

# Run tests with tox
scripts/test -t
scripts/test --tox
```

### Running a Single Test
```bash
# Run a specific test file
python -m unittest tests.test_djvu

# Run a specific test class
python -m unittest tests.test_djvu.TestDjVu

# Run a specific test method
python -m unittest tests.test_djvu.TestDjVu.test_djvu_dump
```

### Code Formatting/Linting
```bash
# Run black and isort on the codebase
scripts/blackisort

# Or run individually:
isort djvuviewer/*.py tests/*.py
black djvuviewer/*.py tests/*.py
```

### Building
```bash
# Build the package with hatchling (configured in pyproject.toml)
pip install build
python -m build
```

## Code Style Guidelines

### General Principles
- Follow PEP 8 with **Black** formatter (line length: 88)
- Use **isort** for import organization
- Use **Google-style docstrings** (as indicated by README badge)
- Use type hints throughout the codebase
- Never use return expressions assign return vars for debugging with meaningful names
- avoid multiple return statements assign the default return value to the return var and flow to the end if
the code does not get to complex
  # Bad: return f(g(h(i(pi*3),l(x))
  # Good:
  flight_area=...
  return flight_area



### Imports Organization
Order imports in this sequence with blank lines between groups:
1. Standard library imports (`datetime`, `os`, `pathlib`, etc.)
2. Third-party imports (`numpy`, `fastapi`, `basemkit`, etc.)
3. Local imports (`from djvuviewer...`)

```python
# Example import organization:
import datetime
import os
from pathlib import Path
from typing import List, Optional

import numpy
from basemkit.yamlable import lod_storable
from fastapi import FastAPI

from djvuviewer.packager import Packager
```

### Type Hints
- Use `Optional[T]` instead of `Union[T, None]`
- Use type hints for all function parameters and return types
- Use `List[T]` from typing (not `list[T]` for Python 3.10 compatibility)

```python
def get_page_by_page_index(self, page_index: int) -> Optional[DjVuPage]:
    ...

def formatted_date(self, date_format: str = "%Y-%m-%d %H:%M") -> str:
    ...
```

### Naming Conventions
- **Classes**: PascalCase (`DjVuViewer`, `DjVuCmd`, `ImageJob`)
- **Functions/methods**: snake_case (`get_fileinfo`, `handle_args`)
- **Variables**: snake_case (`djvu_path`, `page_index`)
- **Constants**: SCREAMING_SNAKE_CASE (e.g., `PACKAGE_MODE`)
- **Private methods**: prefix with underscore (`_internal_method`)

### Dataclasses
Use `@dataclass` decorators for data models with `field` for defaults:
```python
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class DjVuPage(BaseFile):
    path: str
    page_index: int
    valid: bool = False
    width: Optional[int] = field(default=None, kw_only=True)
```

### Error Handling
- Use try/except with specific exception types
- Use logging for error messages
- Include context in error messages

```python
try:
    dt = datetime.datetime.fromisoformat(self.iso_date)
    fmt_date = dt.strftime(date_format)
except (ValueError, AttributeError):
    fmt_date = self.iso_date
```

### Docstrings
Use Google-style docstrings with Args and Returns sections:

```python
def get_fileinfo(filepath: str):
    """
    Retrieve file metadata.

    Args:
        filepath: Path to the file

    Returns:
        Tuple of (iso_date, filesize)
    """
```

### Testing Conventions
- Inherit from `basemkit.basetest.Basetest`
- Use `setUp(self, debug=True, profile=True)` method
- Use `self.assertEqual`, `self.assertIsNone`, etc. from unittest
- Use `self.subTest()` for parameterized tests
- Debug output via `self.debug` flag

```python
class TestDjVu(Basetest):
    def setUp(self, debug=True, profile=True):
        Basetest.setUp(self, debug=debug, profile=profile)
        # Test setup code

    def test_example(self):
        """Test example"""
        with self.subTest(param=value):
            self.assertEqual(expected, actual)
```

### Project Structure
```
djvuviewer/
    __init__.py       # Version only: __version__ = "x.y.z"
    djvu_*.py         # Core modules
    *.py              # Utilities
tests/
    test_*.py         # Test files
scripts/
    test              # Test runner
    blackisort        # Formatter
```

### CLI Patterns
- Use `argparse` for command-line arguments
- Inherit from `basemkit.base_cmd.BaseCmd`
- Define commands in `add_arguments()` method

```python
class DjVuCmd(BaseCmd):
    def __init__(self, args: argparse.Namespace):
        super().__init__(Version())
        self.args = args

    def add_arguments(self, parser: ArgumentParser) -> ArgumentParser:
        super().add_arguments(parser)
        parser.add_argument("--command", choices=["catalog", "convert", ...])
        return parser
```

### Configuration
- Use `pyproject.toml` for project metadata and dependencies
- Version managed via `hatchling` in `djvuviewer/__init__.py`
- Optional test dependencies under `[project.optional-dependencies]`

### Fix Data at the Source — NEVER Patch Presentation

**CRITICAL RULE: Fix data problems where the data originates, not in display/formatting code.**
- If a query returns dates in the wrong format, fix the SQL query or the data model
- DO NOT add formatting helpers, wrapper methods, or conversion logic in `show_info` or any display layer
- DO NOT add utility methods (e.g. `iso_date()`) to CLI/presentation classes to paper over bad data
- DO NOT introduce new helper functions, static methods, or imports to work around a data format issue
- When in doubt about where the fix belongs — STOP and ASK THE USER

### Named Parameterized Queries — STAY AT ABSTRACTION LEVEL

This project uses the **Named Parameterized Query** abstraction from `lodstorage` / `pylodstorage`.
Queries are defined by name in YAML files and executed via `MultiLanguageQueryManager` and `DjVuManager`.

**CRITICAL RULE: Never bypass the abstraction. When in doubt ASK THE USER.**
- DO NOT inspect raw `Query` object internals
- DO NOT wire up database connections manually
- DO NOT use raw `SQLDB`, `mysql.connector`, `pymysql` or any driver directly
- DO use `DjVuManager.query(query_name, param_dict)` for SQLite queries
- DO get the SQL string via `q = mlqm.query4Name(name)` then pass `q.query` to `get_sql_backend(endpoint).query(q.query)`
- DO use `get_sql_backend()` from `lodstorage.sql_backend` with an `Endpoint` from `EndpointManager.of_yaml()` for MariaDB
- DO add new SQL queries as named entries in the appropriate YAML file (`djvu_queries.yaml` or `wiki_queries.yaml`)
- DO configure the endpoint name in `DjVuConfig.wiki_endpoint`
- When you do not know how to execute a query at the abstraction level — STOP and ASK THE USER

## Dependencies
- **pybasemkit**: Base classes, YAML/JSON I/O, CLI tooling
- **ngwidgets**: UI components (includes NiceGUI/FastAPI)
- **py-3rdparty-mediawiki**: MediaWiki API access
- **djvulibre-python**: DjVu file handling
- **pillow**: Image processing
