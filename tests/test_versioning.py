"""Tests for semantic version handling."""

from __future__ import annotations

import pytest

from tessera.core.exceptions import VersionError
from tessera.core.versioning import VersionManager


def test_first_version_defaults_to_1_0_0():
    manager = VersionManager()

    assert manager.next_version(None) == "1.0.0"


def test_minor_major_and_patch_bumps():
    manager = VersionManager()

    assert manager.next_version("1.0.0", "minor") == "1.1.0"
    assert manager.next_version("1.0.0", "major") == "2.0.0"
    assert manager.next_version("1.0.0", "patch") == "1.0.1"


def test_version_compare():
    manager = VersionManager()

    assert manager.compare("1.0.0", "1.0.1") == -1
    assert manager.compare("2.0.0", "1.9.9") == 1
    assert manager.compare("1.2.3", "1.2.3") == 0


def test_invalid_version_raises():
    manager = VersionManager()

    with pytest.raises(VersionError):
        manager.parse("1.0")

    with pytest.raises(VersionError):
        manager.next_version("1.0.0", "invalid")
