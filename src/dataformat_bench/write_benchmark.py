"""Write benchmark - streaming data generation and writing."""

import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from tqdm import tqdm

from .config import DATA_DIR
from .formats.base import FormatHandler
from .generator import OrderGenerator


@dataclass
class WriteResult:
    """Results from write benchmark."""

    format_name: str
    file_size_bytes: int
    write_time_seconds: float
    record_count: int
    file_path: str


class WriteBenchmark:
    """Benchmark for streaming write operations."""

    def __init__(self, output_dir: Path = DATA_DIR):
        """Initialize write benchmark.

        Args:
            output_dir: Directory for output files
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run_single_format(
        self,
        handler: FormatHandler,
        total_records: int,
        seed: int | None = None,
        base_filename: str = "benchmark_data",
    ) -> WriteResult:
        """Run write benchmark for a single format with streaming.

        Args:
            handler: Format handler
            total_records: Total number of records to generate
            seed: Random seed for reproducibility
            base_filename: Base filename (without extension)

        Returns:
            WriteResult with metrics
        """
        print(f"\n{'='*60}")
        print(f"Writing {handler.format_name.upper()} (streaming mode)")
        print(f"{'='*60}")

        file_path = handler.get_file_path(self.output_dir / base_filename)

        # Clean up if exists
        if file_path.exists():
            file_path.unlink()

        # Generate data stream
        generator = OrderGenerator(seed=seed)
        data_stream = generator.generate_stream(total_records)

        # Progress tracking
        pbar = tqdm(total=total_records, desc=f"Writing {handler.format_name}")

        def progress_callback(count: int):
            pbar.n = count
            pbar.refresh()

        # Measure write time
        start_time = time.perf_counter()
        records_written = handler.write_streaming(
            data_stream, file_path, progress_callback
        )
        end_time = time.perf_counter()

        pbar.close()

        write_time = end_time - start_time
        file_size = os.path.getsize(file_path)

        print(f"✓ Wrote {records_written:,} records in {write_time:.2f}s")
        print(f"✓ File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
        print(f"✓ Throughput: {records_written / write_time:.0f} records/sec")

        return WriteResult(
            format_name=handler.format_name,
            file_size_bytes=file_size,
            write_time_seconds=write_time,
            record_count=records_written,
            file_path=str(file_path),
        )

    def run_all_formats(
        self,
        handlers: list[FormatHandler],
        total_records: int,
        seed: int | None = None,
    ) -> list[WriteResult]:
        """Run write benchmark for all formats.

        Args:
            handlers: List of format handlers
            total_records: Total number of records per format
            seed: Random seed

        Returns:
            List of WriteResult for each format
        """
        print(f"\n📝 Write Benchmark")
        print(f"Total records per format: {total_records:,}")
        print(f"Formats: {', '.join(h.format_name for h in handlers)}")
        if seed is not None:
            print(f"Seed: {seed}")

        results = []
        for handler in handlers:
            result = self.run_single_format(handler, total_records, seed)
            results.append(result)

        return results

    def save_results(self, results: list[WriteResult], output_path: Path):
        """Save write results to JSON.

        Args:
            results: List of write results
            output_path: Path to output JSON file
        """
        data = [asdict(r) for r in results]
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n💾 Write results saved to: {output_path}")

    @staticmethod
    def load_results(input_path: Path) -> list[WriteResult]:
        """Load write results from JSON.

        Args:
            input_path: Path to input JSON file

        Returns:
            List of WriteResult
        """
        with open(input_path, "r") as f:
            data = json.load(f)
        return [WriteResult(**item) for item in data]
