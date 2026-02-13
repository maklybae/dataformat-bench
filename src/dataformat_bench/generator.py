"""Data generator for e-commerce orders."""

import random
import uuid
from datetime import datetime, timedelta
from typing import Iterator

from faker import Faker

from .config import BATCH_SIZE, CATEGORIES, PAYMENT_METHODS, SHIPPING_COUNTRIES
from .schema import Order


class OrderGenerator:
    """Generator for realistic e-commerce order data."""

    def __init__(self, seed: int | None = None):
        """Initialize the generator with optional seed for reproducibility.

        Args:
            seed: Random seed for reproducible data generation
        """
        self.seed = seed
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        self.faker = Faker()

    def generate_single(self) -> Order:
        """Generate a single order record.

        Returns:
            Order instance with randomly generated data
        """
        quantity = random.randint(1, 10)
        price = round(random.uniform(5.0, 500.0), 2)
        total_amount = round(quantity * price, 2)

        # Random date within the last 2 years
        days_ago = random.randint(0, 730)
        order_date = datetime.now() - timedelta(days=days_ago)

        return Order(
            order_id=str(uuid.uuid4()),
            customer_id=random.randint(1, 1_000_000),
            product_id=random.randint(1, 100_000),
            product_name=self.faker.catch_phrase(),
            category=random.choice(CATEGORIES),
            quantity=quantity,
            price=price,
            total_amount=total_amount,
            order_date=order_date,
            shipping_country=random.choice(SHIPPING_COUNTRIES),
            payment_method=random.choice(PAYMENT_METHODS),
            is_returned=random.random() < 0.05,  # 5% return rate
        )

    def generate_batch(self, size: int = BATCH_SIZE) -> list[Order]:
        """Generate a batch of order records.

        Args:
            size: Number of orders to generate in this batch

        Returns:
            List of Order instances
        """
        return [self.generate_single() for _ in range(size)]

    def generate_stream(self, total_records: int) -> Iterator[list[Order]]:
        """Generate orders in batches as a stream.

        Args:
            total_records: Total number of records to generate

        Yields:
            Batches of Order instances
        """
        remaining = total_records
        while remaining > 0:
            batch_size = min(BATCH_SIZE, remaining)
            yield self.generate_batch(batch_size)
            remaining -= batch_size

    def estimate_records_for_size(self, target_size_gb: float) -> int:
        """Estimate number of records needed to reach target size.

        Args:
            target_size_gb: Target size in gigabytes

        Returns:
            Estimated number of records
        """
        # Average record size is approximately 200 bytes in memory
        # This will vary by format, but gives a starting point
        avg_record_size_bytes = 200
        target_size_bytes = target_size_gb * 1024 * 1024 * 1024
        return int(target_size_bytes / avg_record_size_bytes)
