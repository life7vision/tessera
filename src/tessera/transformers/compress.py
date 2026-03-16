"""Compression transformer implementation."""

from __future__ import annotations

import gzip
import time
from pathlib import Path

import zstandard as zstd

from tessera.core.hashing import compute_file_checksum
from tessera.transformers.base import BaseTransformer, TransformResult


class CompressTransformer(BaseTransformer):
    """Compress files with supported algorithms."""

    name = "compress"
    version = "0.1.0"
    output_format = "compressed"
    already_compressed_suffixes = {".gz", ".zst", ".zip"}

    def transform(self, input_path: Path, output_path: Path, **kwargs) -> TransformResult:
        start = time.perf_counter()
        algorithm = kwargs.get("algorithm") or self.config.get("compression", "zstd")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if input_path.suffix in self.already_compressed_suffixes:
            output_path.write_bytes(input_path.read_bytes())
        elif algorithm == "gzip":
            with input_path.open("rb") as src, gzip.open(output_path, "wb") as dst:
                dst.write(src.read())
        else:
            level = int(self.config.get("compression_level", 3))
            compressor = zstd.ZstdCompressor(level=level)
            with input_path.open("rb") as src, output_path.open("wb") as dst:
                dst.write(compressor.compress(src.read()))

        return TransformResult(
            transformer_name=self.name,
            success=True,
            input_path=input_path,
            output_path=output_path,
            input_checksum=compute_file_checksum(input_path),
            output_checksum=compute_file_checksum(output_path),
            input_size=input_path.stat().st_size,
            output_size=output_path.stat().st_size,
            duration_ms=int((time.perf_counter() - start) * 1000),
            details={"algorithm": algorithm},
        )

