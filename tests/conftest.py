"""Shared fixtures: load each service's main.py under a unique module name."""

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_service(service: str) -> ModuleType:
    # Every service has its own main.py, so they cannot be imported by name.
    spec = importlib.util.spec_from_file_location(
        f'{service}_main', REPO_ROOT / service / 'main.py'
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def gen() -> ModuleType:
    return load_service('generator')


@pytest.fixture
def proc() -> ModuleType:
    return load_service('processor')


@pytest.fixture
def api() -> ModuleType:
    return load_service('api')
