"""Custom exceptions for EIA Data Connector."""


class EIAConnectorError(Exception):
    """Base exception for EIA Connector."""

    pass


class InvalidAPIKeyError(EIAConnectorError):
    """Raised when API key is missing or invalid."""

    pass


class APIError(EIAConnectorError):
    """Raised when API returns an error."""

    pass


class NetworkError(EIAConnectorError):
    """Raised when network request fails."""

    pass


class DataValidationError(EIAConnectorError):
    """Raised when record validation fails."""

    pass
