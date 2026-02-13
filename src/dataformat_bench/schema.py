"""Data schema definitions."""

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass
class Order:
    """E-commerce order data structure."""

    order_id: str
    customer_id: int
    product_id: int
    product_name: str
    category: str
    quantity: int
    price: float
    total_amount: float
    order_date: datetime
    shipping_country: str
    payment_method: str
    is_returned: bool

    def to_dict(self) -> dict[str, Any]:
        """Convert order to dictionary with serializable types."""
        data = asdict(self)
        data["order_date"] = int(self.order_date.timestamp() * 1000)  # milliseconds
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Order":
        """Create Order from dictionary."""
        data = data.copy()
        if isinstance(data["order_date"], int):
            data["order_date"] = datetime.fromtimestamp(data["order_date"] / 1000)
        return cls(**data)

    def to_avro_dict(self) -> dict[str, Any]:
        """Convert to Avro-compatible dictionary."""
        return {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "product_id": self.product_id,
            "product_name": self.product_name,
            "category": self.category,
            "quantity": self.quantity,
            "price": self.price,
            "total_amount": self.total_amount,
            "order_date": int(self.order_date.timestamp() * 1000),
            "shipping_country": self.shipping_country,
            "payment_method": self.payment_method,
            "is_returned": self.is_returned,
        }


# Avro schema definition
AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "dataformat_bench",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "long"},
        {"name": "product_id", "type": "long"},
        {"name": "product_name", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "price", "type": "double"},
        {"name": "total_amount", "type": "double"},
        {"name": "order_date", "type": "long"},
        {"name": "shipping_country", "type": "string"},
        {"name": "payment_method", "type": "string"},
        {"name": "is_returned", "type": "boolean"},
    ],
}
