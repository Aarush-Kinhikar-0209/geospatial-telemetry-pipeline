# Phase 0: Project Setup & Skeleton

## Overview
Before writing any data pipeline logic or configuring infrastructure, we need to establish a clean, professional repository structure. A well-organized codebase is the first thing engineering managers look for. This phase focuses on scaffolding directories, setting up dependency management, and configuring code quality tools.

## Implementation Details

### 1. Directory Scaffolding
Run the following commands in your terminal to create the foundational structure for the microservices:

```bash
mkdir realtime-fleet-analytics
cd realtime-fleet-analytics

# Create directories for each phase/service
mkdir infrastructure  # Docker compose and init scripts
mkdir generator       # Python synthetic data producer
mkdir processor       # Python/Flink stream processing
mkdir api             # FastAPI backend
mkdir ui              # Next.js frontend
mkdir docs            # Project documentation (like this file!)
```

### 2. Global `.gitignore`
Create a `.gitignore` at the root of the project to prevent committing virtual environments, cache files, and sensitive credentials.
* **Key inclusions:** `.venv/`, `__pycache__/`, `node_modules/`, `.env`, `.DS_Store`, `data/` (for local test data).

### 3. Python Environment Setup
We will use standard Python virtual environments and `pip` (or `uv`/`poetry` if you prefer). Create a shared or per-service virtual environment.
For example, inside the `generator` and `api` folders:

```bash
cd generator
python3 -m venv .venv
source .venv/bin/activate
touch requirements.txt
```

### 4. Code Quality & Formatting
Consistent code formatting is a hallmark of senior engineering. 
* Install **Ruff** (a lightning-fast Python linter and formatter) or **Black/Flake8**.
* Create a `pyproject.toml` file at the root to configure your linting rules.
* *(Optional but recommended)* Set up a `.pre-commit-config.yaml` to automatically format code before every git commit.

## Ways to Test
1. **Structure Check:** Run `tree` (or `ls -R` / `dir /s`) in the root directory to verify all folders exist.
2. **Environment Check:** Activate a Python virtual environment and run `python --version` to ensure it is isolated from your system Python.
3. **Linting Check:** Run `ruff check .` or `black --check .` to verify your formatter is installed and working.

## Quality Benchmarks
* The repository contains no hardcoded credentials or `.env` files in version control.
* All Python dependencies are explicitly tracked (e.g., in `requirements.txt` or `pyproject.toml`).

## Acceptance Criteria
- [ ] Root directory and all sub-service folders (`infrastructure`, `generator`, `processor`, `api`, `ui`, `docs`) are created.
- [ ] A comprehensive `.gitignore` is committed to the root.
- [ ] Virtual environments can be successfully created and activated.
- [ ] A code formatter (e.g., Ruff) is configured and tested locally.