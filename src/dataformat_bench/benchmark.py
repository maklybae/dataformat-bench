"""Benchmarking functionality for data formats."""

import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

from tqdm import tqdm

from .config import BENCHMARK_RUNS, DATA_DIR
from .formats.base import FormatHandler
from .generator import OrderGenerator
from .schema import Order


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    format_name: str
    file_size_bytes: int
    write_time_seconds: float
    read_full_time_seconds: float | None
    read_filtered_time_seconds: float | None
    aggregate_time_seconds: float | None
    record_count: int
    read_filtered_time_seconds: float
    aggregate_time_seconds: float
    record_count: int


class Benchmark:
    """Benchmark runner for data format comparison."""

    def __init__(
        self,
        output_dir: Path = DATA_DIR,
        runs: int = BENCHMARK_RUNS,
        filter_category: str = "Electronics",
    ):
        """Initialize benchmark runner.

        Args:
            output_dir: Directory for output files
            runs: Number of times to repeat each measurement
            filter_category: Category to use for filtered read tests
        """
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.runs = runs
        self.filter_category = filter_category

    def _time_operation(
        self, operation: Callable, description: str = ""
    ) -> tuple[float, any]:
        """Time an operation and return duration and result.

        Args:
            operation: Callable to time
            description: Description for progress display

        Returns:
            Tuple of (duration in seconds, operation result)
        """
        start_time = time.perf_counter()
        result = operation()
        end_time = time.perf_counter()
        duration = end_time - start_time
        return duration, result

    def _clear_cache(self):
        """Attempt to clear filesystem cache (best effort)."""
        try:
            # On macOS/Linux, sync to flush buffers
            os.sync()
        except (AttributeError, OSError):
            pass

    def run_format_benchmark(
        self,
        handler: FormatHandler,
        data: list[Order],
        base_filename: str = "benchmark_data",
    ) -> BenchmarkResult:
        """Run complete benchmark for a single format.

        Args:
            handler: Format handler to benchmark
            data: Data to write
            base_filename: Base name for output file

        Returns:
            BenchmarkResult with all metrics
        """
        print(f"\n{'='*60}")
        print(f"Benchmarking {handler.format_name.upper()}")
        print(f"{'='*60}")

        file_path = handler.get_file_path(self.output_dir / base_filename)

        # Clean up previous file if exists
        if file_path.exists():
            file_path.unlink()

        # Measure write time
        print(f"\n📝 Writing {len(data):,} records...")
        write_times = []
        for run in range(self.runs):
            if file_path.exists():
                file_path.unlink()

            duration, _ = self._time_operation(
                lambda: handler.write(data, file_path),
                f"Write run {run + 1}/{self.runs}",
            )
            write_times.append(duration)
            print(f"  Run {run + 1}/{self.runs}: {duration:.2f}s")

        avg_write_time = sum(write_times) / len(write_times)

        # Get file size
        file_size = os.path.getsize(file_path)
        print(f"\n📊 File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")

        # Measure full scan time
        print(f"\n📖 Full scan...")
        read_full_times = []
        for run in range(self.runs):
            self._clear_cache()

            def full_scan():
                count = 0
                for _ in handler.read_full(file_path):
                    count += 1
                return count

            duration, record_count = self._time_operation(full_scan)
            read_full_times.append(duration)
            print(f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({record_count:,} records)")

        avg_read_full_time = sum(read_full_times) / len(read_full_times)

        # Measure filtered read time
        print(f"\n🔍 Filtered read (category='{self.filter_category}')...")
        read_filtered_times = []
        for run in range(self.runs):
            self._clear_cache()

            def filtered_scan():
                count = 0
                for _ in handler.read_filtered(file_path, self.filter_category):
                    count += 1
                return count

            duration, filtered_count = self._time_operation(filtered_scan)
            read_filtered_times.append(duration)
            print(
                f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({filtered_count:,} records)"
            )

        avg_read_filtered_time = sum(read_filtered_times) / len(read_filtered_times)

        # Measure aggregation time
        print(f"\n📈 Aggregation (sum by country)...")
        aggregate_times = []
        for run in range(self.runs):
            self._clear_cache()

            duration, result = self._time_operation(
                lambda: handler.aggregate(file_path)
            )
            aggregate_times.append(duration)
            print(f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({len(result)} countries)")

        avg_aggregate_time = sum(aggregate_times) / len(aggregate_times)

        return BenchmarkResult(
            format_name=handler.format_name,
            file_size_bytes=file_size,
            write_time_seconds=avg_write_time,
            read_full_time_seconds=avg_read_full_time,
            read_filtered_time_seconds=avg_read_filtered_time,
            aggregate_time_seconds=avg_aggregate_time,
            record_count=len(data),
        )

    def run_all_formats(
        self,
        handlers: list[FormatHandler],
        total_records: int,
        seed: int | None = None,
    ) -> list[BenchmarkResult]:
        """Run benchmark for all formats.

        Args:
            handlers: List of format handlers to benchmark
            total_records: Total number of records to generate
            seed: Random seed for reproducibility

        Returns:
            List of BenchmarkResult for each format
        """
        print(f"\n🎲 Generating {total_records:,} records...")
        generator = OrderGenerator(seed=seed)
        data = []

        with tqdm(total=total_records, desc="Generating data") as pbar:
            for batch in generator.generate_stream(total_records):
                data.extend(batch)
                pbar.update(len(batch))

        print(f"✅ Generated {len(data):,} records")

        results = []
        for handler in handlers:
            result = self.run_format_benchmark(handler, data)
            results.append(result)

        return results

    def save_results(self, results: list[BenchmarkResult], output_path: Path):
        """Save benchmark results to JSON file.

        Args:
            results: List of benchmark results
            output_path: Path to output JSON file
        """
        data = [asdict(r) for r in results]
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n💾 Results saved to: {output_path}")

    @staticmethod
    def load_results(input_path: Path) -> list[BenchmarkResult]:
        """Load benchmark results from JSON file.

        Args:
            input_path: Path to input JSON file

        Returns:
            List of BenchmarkResult
        """
        with open(input_path, "r") as f:
            data = json.load(f)
        return [BenchmarkResult(**item) for item in data]
