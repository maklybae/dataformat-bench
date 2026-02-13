"""Abstract base class for format handlers."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Iterator

from ..schema import Order


class FormatHandler(ABC):
    """Abstract base class for data format handlers."""

    @property
    @abstractmethod
    def format_name(self) -> str:
        """Return the name of the format."""

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """Return the file extension for this format."""

    @abstractmethod
    def write(self, data: list[Order], path: Path) -> None:
        """Write data to file in this format.

        Args:
            data: List of Order objects to write
            path: Path to output file
        """

    @abstractmethod
    def write_streaming(
        self,
        data_stream: Iterator[list[Order]],
        path: Path,
        progress_callback: Callable[[int], None] | None = None,
    ) -> int:
        """Write data in streaming mode (memory efficient).

        Args:
            data_stream: Iterator yielding batches of Order objects
            path: Path to output file
            progress_callback: Optional callback called with number of records written

        Returns:
            Total number of records written
        """

    @abstractmethod
    def read_full(self, path: Path) -> Iterator[Order]:
        """Read all records from file (full scan).

        Args:
            path: Path to input file

        Yields:
            Order objects
        """

    @abstractmethod
    def read_filtered(self, path: Path, category: str) -> Iterator[Order]:
        """Read records filtered by category.

        Args:
            path: Path to input file
            category: Category to filter by

        Yields:
            Order objects matching the filter
        """

    @abstractmethod
    def aggregate(self, path: Path) -> dict[str, float]:
        """Aggregate total_amount by shipping_country.

        Args:
            path: Path to input file

        Returns:
            Dictionary mapping country to total amount
        """

    def get_file_path(self, base_path: Path) -> Path:
        """Get the full file path with appropriate extension.

        Args:
            base_path: Base path without extension

        Returns:
            Path with format-specific extension
        """
        return base_path.with_suffix(self.file_extension)
