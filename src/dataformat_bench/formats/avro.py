"""Avro format handler implementation."""

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator

from fastavro import reader, writer

from ..schema import AVRO_SCHEMA, Order
from .base import FormatHandler


class AvroHandler(FormatHandler):
    """Handler for Apache Avro format."""

    @property
    def format_name(self) -> str:
        return "avro"

    @property
    def file_extension(self) -> str:
        return ".avro"

    def _order_to_avro(self, order: Order) -> dict:
        """Convert Order to Avro-compatible dictionary.

        Args:
            order: Order object

        Returns:
            Dictionary with Avro-compatible types
        """
        return order.to_avro_dict()

    def _avro_to_order(self, record: dict) -> Order:
        """Convert Avro record to Order object.

        Args:
            record: Avro record dictionary

        Returns:
            Order object
        """
        return Order(
            order_id=record["order_id"],
            customer_id=record["customer_id"],
            product_id=record["product_id"],
            product_name=record["product_name"],
            category=record["category"],
            quantity=record["quantity"],
            price=record["price"],
            total_amount=record["total_amount"],
            order_date=datetime.fromtimestamp(record["order_date"] / 1000),
            shipping_country=record["shipping_country"],
            payment_method=record["payment_method"],
            is_returned=record["is_returned"],
        )

    def write(self, data: list[Order], path: Path) -> None:
        """Write data to Avro file.

        Args:
            data: List of Order objects to write
            path: Path to output file
        """
        records = [self._order_to_avro(order) for order in data]

        with open(path, "wb") as f:
            writer(f, AVRO_SCHEMA, records, codec="snappy")

    def write_streaming(
        self,
        data_stream: Iterator[list[Order]],
        path: Path,
        progress_callback: Callable[[int], None] | None = None,
    ) -> int:
        """Write data to Avro file in streaming mode.

        Note: fastavro's writer() writes all records at once, so we collect
        batches in memory before writing. This is a limitation of the library.

        Args:
            data_stream: Iterator yielding batches of Order objects
            path: Path to output file
            progress_callback: Optional callback with records written count

        Returns:
            Total number of records written
        """
        all_records = []
        total_records = 0

        for batch in data_stream:
            batch_records = [self._order_to_avro(order) for order in batch]
            all_records.extend(batch_records)
            total_records += len(batch)

            if progress_callback:
                progress_callback(total_records)

        with open(path, "wb") as f:
            writer(f, AVRO_SCHEMA, all_records, codec="snappy")

        return total_records

    def read_full(self, path: Path) -> Iterator[Order]:
        """Read all records from Avro file (full scan).

        Args:
            path: Path to input file

        Yields:
            Order objects
        """
        with open(path, "rb") as f:
            avro_reader = reader(f)
            for record in avro_reader:
                yield self._avro_to_order(record)

    def read_filtered(self, path: Path, category: str) -> Iterator[Order]:
        """Read records filtered by category.

        Note: Avro doesn't support predicate pushdown, so we filter in-memory.

        Args:
            path: Path to input file
            category: Category to filter by

        Yields:
            Order objects matching the filter
        """
        with open(path, "rb") as f:
            avro_reader = reader(f)
            for record in avro_reader:
                if record["category"] == category:
                    yield self._avro_to_order(record)

    def aggregate(self, path: Path) -> dict[str, float]:
        """Aggregate total_amount by shipping_country.

        Args:
            path: Path to input file

        Returns:
            Dictionary mapping country to total amount
        """
        aggregates = defaultdict(float)

        with open(path, "rb") as f:
            avro_reader = reader(f)
            for record in avro_reader:
                country = record["shipping_country"]
                amount = record["total_amount"]
                aggregates[country] += amount

        return dict(aggregates)
