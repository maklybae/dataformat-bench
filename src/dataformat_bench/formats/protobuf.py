"""Protobuf format handler implementation."""

import struct
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator

from ..order_pb2 import Order as ProtoOrder
from ..schema import Order
from .base import FormatHandler


class ProtobufHandler(FormatHandler):
    """Handler for Protocol Buffers format.

    Uses length-prefixed messages for writing multiple records to a single file.
    """

    @property
    def format_name(self) -> str:
        return "protobuf"

    @property
    def file_extension(self) -> str:
        return ".pb"

    def _order_to_proto(self, order: Order) -> ProtoOrder:
        """Convert Order to Protobuf message.

        Args:
            order: Order object

        Returns:
            ProtoOrder message
        """
        proto_order = ProtoOrder()
        proto_order.order_id = order.order_id
        proto_order.customer_id = order.customer_id
        proto_order.product_id = order.product_id
        proto_order.product_name = order.product_name
        proto_order.category = order.category
        proto_order.quantity = order.quantity
        proto_order.price = order.price
        proto_order.total_amount = order.total_amount
        proto_order.order_date = int(order.order_date.timestamp() * 1000)
        proto_order.shipping_country = order.shipping_country
        proto_order.payment_method = order.payment_method
        proto_order.is_returned = order.is_returned
        return proto_order

    def _proto_to_order(self, proto_order: ProtoOrder) -> Order:
        """Convert Protobuf message to Order object.

        Args:
            proto_order: ProtoOrder message

        Returns:
            Order object
        """
        return Order(
            order_id=proto_order.order_id,
            customer_id=proto_order.customer_id,
            product_id=proto_order.product_id,
            product_name=proto_order.product_name,
            category=proto_order.category,
            quantity=proto_order.quantity,
            price=proto_order.price,
            total_amount=proto_order.total_amount,
            order_date=datetime.fromtimestamp(proto_order.order_date / 1000),
            shipping_country=proto_order.shipping_country,
            payment_method=proto_order.payment_method,
            is_returned=proto_order.is_returned,
        )

    def write(self, data: list[Order], path: Path) -> None:
        """Write data to Protobuf file using length-prefixed messages.

        Args:
            data: List of Order objects to write
            path: Path to output file
        """
        with open(path, "wb") as f:
            for order in data:
                proto_order = self._order_to_proto(order)
                serialized = proto_order.SerializeToString()

                # Write length prefix (4 bytes, big-endian)
                length = len(serialized)
                f.write(struct.pack(">I", length))

                # Write serialized message
                f.write(serialized)

    def write_streaming(
        self,
        data_stream: Iterator[list[Order]],
        path: Path,
        progress_callback: Callable[[int], None] | None = None,
    ) -> int:
        """Write data to Protobuf file in streaming mode.

        Protobuf naturally supports streaming with length-prefixed messages.

        Args:
            data_stream: Iterator yielding batches of Order objects
            path: Path to output file
            progress_callback: Optional callback with records written count

        Returns:
            Total number of records written
        """
        total_records = 0

        with open(path, "wb") as f:
            for batch in data_stream:
                for order in batch:
                    proto_order = self._order_to_proto(order)
                    serialized = proto_order.SerializeToString()

                    # Write length prefix (4 bytes, big-endian)
                    length = len(serialized)
                    f.write(struct.pack(">I", length))

                    # Write serialized message
                    f.write(serialized)

                total_records += len(batch)

                if progress_callback:
                    progress_callback(total_records)

        return total_records

    def _read_messages(self, path: Path) -> Iterator[ProtoOrder]:
        """Read length-prefixed Protobuf messages from file.

        Args:
            path: Path to input file

        Yields:
            ProtoOrder messages
        """
        with open(path, "rb") as f:
            while True:
                # Read length prefix
                length_bytes = f.read(4)
                if not length_bytes:
                    break

                length = struct.unpack(">I", length_bytes)[0]

                # Read message
                message_bytes = f.read(length)
                if len(message_bytes) != length:
                    break

                proto_order = ProtoOrder()
                proto_order.ParseFromString(message_bytes)
                yield proto_order

    def read_full(self, path: Path) -> Iterator[Order]:
        """Read all records from Protobuf file (full scan).

        Args:
            path: Path to input file

        Yields:
            Order objects
        """
        for proto_order in self._read_messages(path):
            yield self._proto_to_order(proto_order)

    def read_filtered(self, path: Path, category: str) -> Iterator[Order]:
        """Read records filtered by category.

        Note: Protobuf doesn't support predicate pushdown, so we filter in-memory.

        Args:
            path: Path to input file
            category: Category to filter by

        Yields:
            Order objects matching the filter
        """
        for proto_order in self._read_messages(path):
            if proto_order.category == category:
                yield self._proto_to_order(proto_order)

    def aggregate(self, path: Path) -> dict[str, float]:
        """Aggregate total_amount by shipping_country.

        Args:
            path: Path to input file

        Returns:
            Dictionary mapping country to total amount
        """
        aggregates = defaultdict(float)

        for proto_order in self._read_messages(path):
            country = proto_order.shipping_country
            amount = proto_order.total_amount
            aggregates[country] += amount

        return dict(aggregates)
