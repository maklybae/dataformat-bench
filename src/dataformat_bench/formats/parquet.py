"""Parquet format handler implementation."""

from pathlib import Path
from typing import Callable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq

from ..schema import Order
from .base import FormatHandler


class ParquetHandler(FormatHandler):
    """Handler for Apache Parquet format."""

    @property
    def format_name(self) -> str:
        return "parquet"

    @property
    def file_extension(self) -> str:
        return ".parquet"

    def _orders_to_table(self, orders: list[Order]) -> pa.Table:
        """Convert list of orders to PyArrow table.

        Args:
            orders: List of Order objects

        Returns:
            PyArrow Table
        """
        data = {
            "order_id": [o.order_id for o in orders],
            "customer_id": [o.customer_id for o in orders],
            "product_id": [o.product_id for o in orders],
            "product_name": [o.product_name for o in orders],
            "category": [o.category for o in orders],
            "quantity": [o.quantity for o in orders],
            "price": [o.price for o in orders],
            "total_amount": [o.total_amount for o in orders],
            "order_date": [o.order_date for o in orders],
            "shipping_country": [o.shipping_country for o in orders],
            "payment_method": [o.payment_method for o in orders],
            "is_returned": [o.is_returned for o in orders],
        }
        return pa.table(data)

    def _table_to_orders(self, table: pa.Table) -> Iterator[Order]:
        """Convert PyArrow table to Order objects.

        Args:
            table: PyArrow Table

        Yields:
            Order objects
        """
        for batch in table.to_batches():
            df = batch.to_pandas()
            for _, row in df.iterrows():
                yield Order(
                    order_id=row["order_id"],
                    customer_id=row["customer_id"],
                    product_id=row["product_id"],
                    product_name=row["product_name"],
                    category=row["category"],
                    quantity=row["quantity"],
                    price=row["price"],
                    total_amount=row["total_amount"],
                    order_date=row["order_date"].to_pydatetime(),
                    shipping_country=row["shipping_country"],
                    payment_method=row["payment_method"],
                    is_returned=row["is_returned"],
                )

    def write(self, data: list[Order], path: Path) -> None:
        """Write data to Parquet file.

        Args:
            data: List of Order objects to write
            path: Path to output file
        """
        table = self._orders_to_table(data)
        pq.write_table(
            table,
            path,
            compression="snappy",
            row_group_size=100_000,
        )

    def write_streaming(
        self,
        data_stream: Iterator[list[Order]],
        path: Path,
        progress_callback: Callable[[int], None] | None = None,
    ) -> int:
        """Write data to Parquet file in streaming mode.

        Uses ParquetWriter to append batches incrementally.

        Args:
            data_stream: Iterator yielding batches of Order objects
            path: Path to output file
            progress_callback: Optional callback with records written count

        Returns:
            Total number of records written
        """
        writer = None
        total_records = 0

        try:
            for batch in data_stream:
                if not batch:
                    continue

                table = self._orders_to_table(batch)

                if writer is None:
                    # Initialize writer with schema from first batch
                    writer = pq.ParquetWriter(
                        path,
                        table.schema,
                        compression="snappy",
                        write_batch_size=100_000,
                    )

                writer.write_table(table)
                total_records += len(batch)

                if progress_callback:
                    progress_callback(total_records)
        finally:
            if writer is not None:
                writer.close()

        return total_records

    def read_full(self, path: Path) -> Iterator[Order]:
        """Read all records from Parquet file (full scan).

        Args:
            path: Path to input file

        Yields:
            Order objects
        """
        table = pq.read_table(path)
        yield from self._table_to_orders(table)

    def read_filtered(self, path: Path, category: str) -> Iterator[Order]:
        """Read records filtered by category using predicate pushdown.

        Args:
            path: Path to input file
            category: Category to filter by

        Yields:
            Order objects matching the filter
        """
        # Use Parquet's predicate pushdown for efficient filtering
        table = pq.read_table(
            path,
            filters=[("category", "=", category)],
        )
        yield from self._table_to_orders(table)

    def aggregate(self, path: Path) -> dict[str, float]:
        """Aggregate total_amount by shipping_country using columnar operations.

        Args:
            path: Path to input file

        Returns:
            Dictionary mapping country to total amount
        """
        # Read only necessary columns for efficiency
        table = pq.read_table(
            path,
            columns=["shipping_country", "total_amount"],
        )

        # Convert to pandas for easy groupby
        df = table.to_pandas()
        result = df.groupby("shipping_country")["total_amount"].sum().to_dict()
        return result
