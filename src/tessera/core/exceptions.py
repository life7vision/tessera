"""Project exception hierarchy."""


class TesseraError(Exception):
    """Base exception for all Tessera errors."""


class ConfigError(TesseraError):
    """Raised when configuration loading or validation fails."""


class ConnectorError(TesseraError):
    """Raised when connector operations fail."""


class ValidationError(TesseraError):
    """Raised when validation steps fail."""


class TransformError(TesseraError):
    """Raised when transformation steps fail."""


class StorageError(TesseraError):
    """Raised when storage operations fail."""


class CatalogError(TesseraError):
    """Raised when catalog operations fail."""


class VersionError(TesseraError):
    """Raised when version handling fails."""


class PipelineError(TesseraError):
    """Raised when pipeline orchestration fails."""


class PluginNotFoundError(TesseraError):
    """Raised when a requested plugin is unavailable."""


class DuplicateDatasetError(TesseraError):
    """Raised when an already registered dataset is detected."""


class QuarantineError(TesseraError):
    """Raised when quarantine handling fails."""

