"""Configuration and constants for the benchmark."""

from pathlib import Path

# Directory paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROTO_DIR = PROJECT_ROOT / "proto"

# Benchmark configuration
DEFAULT_TARGET_SIZE_GB = 10
BATCH_SIZE = 100_000  # Number of records to generate at once
BENCHMARK_RUNS = 3  # Number of times to repeat each measurement

# Data generation configuration
CATEGORIES = [
    "Electronics",
    "Books",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Toys",
    "Food & Beverage",
    "Health & Beauty",
    "Automotive",
    "Office Supplies",
    "Pet Supplies",
    "Jewelry",
    "Music",
    "Movies",
    "Video Games",
    "Baby Products",
    "Tools",
    "Outdoor",
    "Furniture",
    "Industrial",
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "paypal",
    "bank_transfer",
    "cash_on_delivery",
]

# Countries for shipping (50 countries)
SHIPPING_COUNTRIES = [
    "United States",
    "China",
    "Japan",
    "Germany",
    "United Kingdom",
    "France",
    "India",
    "Italy",
    "Brazil",
    "Canada",
    "Russia",
    "South Korea",
    "Spain",
    "Australia",
    "Mexico",
    "Indonesia",
    "Netherlands",
    "Saudi Arabia",
    "Turkey",
    "Switzerland",
    "Poland",
    "Belgium",
    "Sweden",
    "Argentina",
    "Norway",
    "Austria",
    "United Arab Emirates",
    "Nigeria",
    "Israel",
    "Ireland",
    "Denmark",
    "Singapore",
    "Malaysia",
    "South Africa",
    "Colombia",
    "Philippines",
    "Pakistan",
    "Chile",
    "Finland",
    "Bangladesh",
    "Egypt",
    "Vietnam",
    "Czech Republic",
    "Portugal",
    "Romania",
    "Peru",
    "Greece",
    "New Zealand",
    "Qatar",
    "Hungary",
]

# Format file extensions
FORMAT_EXTENSIONS = {
    "parquet": ".parquet",
    "avro": ".avro",
    "protobuf": ".pb",
}
