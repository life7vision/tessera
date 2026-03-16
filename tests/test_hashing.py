"""Tests for checksum utilities."""

from __future__ import annotations

import hashlib

import pytest

from tessera.core.exceptions import ValidationError
from tessera.core.hashing import (
    compute_directory_checksum,
    compute_file_checksum,
    verify_checksum,
)


def test_compute_file_checksum(sample_csv):
    expected = hashlib.sha256(sample_csv.read_bytes()).hexdigest()

    assert compute_file_checksum(sample_csv) == expected


def test_verify_checksum(sample_csv):
    checksum = compute_file_checksum(sample_csv)

    assert verify_checksum(sample_csv, checksum) is True
    assert verify_checksum(sample_csv, "deadbeef") is False


def test_compute_directory_checksum_is_stable(tmp_path):
    first = tmp_path / "b.txt"
    second = tmp_path / "a.txt"
    first.write_text("beta", encoding="utf-8")
    second.write_text("alpha", encoding="utf-8")

    checksum_one = compute_directory_checksum(tmp_path)
    checksum_two = compute_directory_checksum(tmp_path)

    assert checksum_one == checksum_two


def test_compute_file_checksum_missing_file_raises(tmp_path):
    with pytest.raises(ValidationError):
        compute_file_checksum(tmp_path / "missing.csv")

