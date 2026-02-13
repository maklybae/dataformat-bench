"""Read benchmark - testing read performance on existing files."""

import json
import os
import signal
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from .config import BENCHMARK_RUNS, DATA_DIR
from .formats.base import FormatHandler


class TimeoutError(Exception):
    """Raised when operation exceeds timeout."""

    pass


@dataclass
class ReadResult:
    """Results from read benchmark."""

    format_name: str
    read_full_time_seconds: float | None
    read_full_timeout: bool
    read_filtered_time_seconds: float | None
    read_filtered_timeout: bool
    aggregate_time_seconds: float | None
    aggregate_timeout: bool
    file_size_bytes: int
    record_count: int | None


class ReadBenchmark:
    """Benchmark for read operations on existing files."""

    def __init__(
        self,
        input_dir: Path = DATA_DIR,
        runs: int = BENCHMARK_RUNS,
        filter_category: str = "Electronics",
        timeout_seconds: int = 600,  # 10 minutes default
    ):
        """Initialize read benchmark.

        Args:
            input_dir: Directory containing data files
            runs: Number of times to repeat each test
            filter_category: Category for filtered read tests
            timeout_seconds: Timeout for each operation in seconds (default: 600 = 10min)
        """
        self.input_dir = input_dir
        self.runs = runs
        self.filter_category = filter_category
        self.timeout_seconds = timeout_seconds

    def _timeout_handler(self, signum, frame):
        """Signal handler for timeout."""
        raise TimeoutError("Operation timed out")

    def _time_operation_with_timeout(
        self, operation, timeout: int
    ) -> tuple[float | None, any, bool]:
        """Time an operation with timeout.

        Args:
            operation: Callable to time
            timeout: Timeout in seconds

        Returns:
            Tuple of (duration in seconds or None, result or None, timed_out flag)
        """
        # Set up signal handler for timeout
        old_handler = signal.signal(signal.SIGALRM, self._timeout_handler)
        signal.alarm(timeout)

        start_time = time.perf_counter()
        timed_out = False
        result = None

        try:
            result = operation()
            duration = time.perf_counter() - start_time
        except TimeoutError:
            duration = None
            timed_out = True
        finally:
            # Cancel alarm and restore old handler
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        return duration, result, timed_out

    def _clear_cache(self):
        """Attempt to clear filesystem cache."""
        try:
            os.sync()
        except (AttributeError, OSError):
            pass

    def run_single_format(
        self,
        handler: FormatHandler,
        base_filename: str = "benchmark_data",
    ) -> ReadResult:
        """Run read benchmark for a single format.

        Args:
            handler: Format handler
            base_filename: Base filename (without extension)

        Returns:
            ReadResult with metrics
        """
        print(f"\n{'='*60}")
        print(f"Reading {handler.format_name.upper()}")
        print(f"{'='*60}")

        file_path = handler.get_file_path(self.input_dir / base_filename)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        print(f"📊 File size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
        print(f"⏱️  Timeout per operation: {self.timeout_seconds}s ({self.timeout_seconds / 60:.1f} min)")

        # Measure full scan
        print(f"\n📖 Full scan ({self.runs} runs, timeout: {self.timeout_seconds}s)...")
        read_full_times = []
        record_count = None
        full_scan_timeout = False

        for run in range(self.runs):
            self._clear_cache()

            def full_scan():
                count = 0
                for _ in handler.read_full(file_path):
                    count += 1
                return count

            duration, count, timed_out = self._time_operation_with_timeout(
                full_scan, self.timeout_seconds
            )

            if timed_out:
                print(f"  Run {run + 1}/{self.runs}: ⏱️ TIMEOUT (>{self.timeout_seconds}s)")
                full_scan_timeout = True
                break
            else:
                read_full_times.append(duration)
                record_count = count
                print(f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({count:,} records)")

        avg_read_full = (
            sum(read_full_times) / len(read_full_times) if read_full_times else None
        )

        # Measure filtered read
        print(
            f"\n🔍 Filtered read (category='{self.filter_category}', {self.runs} runs, timeout: {self.timeout_seconds}s)..."
        )
        read_filtered_times = []
        filtered_read_timeout = False

        for run in range(self.runs):
            self._clear_cache()

            def filtered_scan():
                count = 0
                for _ in handler.read_filtered(file_path, self.filter_category):
                    count += 1
                return count

            duration, filtered_count, timed_out = self._time_operation_with_timeout(
                filtered_scan, self.timeout_seconds
            )

            if timed_out:
                print(f"  Run {run + 1}/{self.runs}: ⏱️ TIMEOUT (>{self.timeout_seconds}s)")
                filtered_read_timeout = True
                break
            else:
                read_filtered_times.append(duration)
                print(
                    f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({filtered_count:,} records)"
                )

        avg_read_filtered = (
            sum(read_filtered_times) / len(read_filtered_times)
            if read_filtered_times
            else None
        )

        # Measure aggregation
        print(f"\n📈 Aggregation (sum by country, {self.runs} runs, timeout: {self.timeout_seconds}s)...")
        aggregate_times = []
        aggregate_timeout = False

        for run in range(self.runs):
            self._clear_cache()

            duration, result, timed_out = self._time_operation_with_timeout(
                lambda: handler.aggregate(file_path), self.timeout_seconds
            )

            if timed_out:
                print(f"  Run {run + 1}/{self.runs}: ⏱️ TIMEOUT (>{self.timeout_seconds}s)")
                aggregate_timeout = True
                break
            else:
                aggregate_times.append(duration)
                print(
                    f"  Run {run + 1}/{self.runs}: {duration:.2f}s ({len(result)} countries)"
                )

        avg_aggregate = (
            sum(aggregate_times) / len(aggregate_times) if aggregate_times else None
        )

        # Print summary
        print(f"\n{'='*60}")
        if avg_read_full is not None:
            print(f"✓ Average full scan: {avg_read_full:.2f}s")
        else:
            print(f"⏱️  Full scan: TIMEOUT")

        if avg_read_filtered is not None:
            print(f"✓ Average filtered read: {avg_read_filtered:.2f}s")
        else:
            print(f"⏱️  Filtered read: TIMEOUT")

        if avg_aggregate is not None:
            print(f"✓ Average aggregation: {avg_aggregate:.2f}s")
        else:
            print(f"⏱️  Aggregation: TIMEOUT")

        return ReadResult(
            format_name=handler.format_name,
            read_full_time_seconds=avg_read_full,
            read_full_timeout=full_scan_timeout,
            read_filtered_time_seconds=avg_read_filtered,
            read_filtered_timeout=filtered_read_timeout,
            aggregate_time_seconds=avg_aggregate,
            aggregate_timeout=aggregate_timeout,
            file_size_bytes=file_size,
            record_count=record_count,
        )

    def run_all_formats(
        self,
        handlers: list[FormatHandler],
    ) -> list[ReadResult]:
        """Run read benchmark for all formats.

        Args:
            handlers: List of format handlers

        Returns:
            List of ReadResult for each format
        """
        print(f"\n📖 Read Benchmark")
        print(f"Formats: {', '.join(h.format_name for h in handlers)}")
        print(f"Runs per test: {self.runs}")
        print(f"Filter category: {self.filter_category}")

        results = []
        for handler in handlers:
            result = self.run_single_format(handler)
            results.append(result)

        return results

    def save_results(self, results: list[ReadResult], output_path: Path):
        """Save read results to JSON.

        Args:
            results: List of read results
            output_path: Path to output JSON file
        """
        data = [asdict(r) for r in results]
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n💾 Read results saved to: {output_path}")

    @staticmethod
    def load_results(input_path: Path) -> list[ReadResult]:
        """Load read results from JSON.

        Args:
            input_path: Path to input JSON file

        Returns:
            List of ReadResult
        """
        with open(input_path, "r") as f:
            data = json.load(f)
        return [ReadResult(**item) for item in data]
