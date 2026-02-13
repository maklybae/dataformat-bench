"""Command-line interface for data format benchmark.

Memory Management:
------------------
New architecture with separate write and read phases minimizes memory usage:
- write: ~200MB RAM (generates and writes in 100K batches)
- read: ~100MB RAM (streaming read operations)

For large datasets (>5GB), use the new phased approach.

Example:
    # Phase 1: Write data (memory efficient)
    uv run dataformat-bench write --size 10

    # Phase 2: Read benchmark
    uv run dataformat-bench read --runs 3

    # Phase 3: Generate report
    uv run dataformat-bench report
"""

from pathlib import Path

import click

from .config import DATA_DIR, DEFAULT_TARGET_SIZE_GB
from .formats.avro import AvroHandler
from .formats.parquet import ParquetHandler
from .formats.protobuf import ProtobufHandler
from .generator import OrderGenerator
from .read_benchmark import ReadBenchmark
from .report import ReportGenerator
from .write_benchmark import WriteBenchmark


FORMAT_MAP = {
    "parquet": ParquetHandler,
    "avro": AvroHandler,
    "protobuf": ProtobufHandler,
}


@click.group()
@click.version_option(version="0.1.0")
def main():
    """Data Format Benchmark - Compare Parquet, Avro, and Protobuf performance."""
    pass


@main.command()
@click.option(
    "--size",
    "-s",
    type=float,
    default=DEFAULT_TARGET_SIZE_GB,
    help=f"Target dataset size in GB (default: {DEFAULT_TARGET_SIZE_GB})",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=DATA_DIR,
    help="Output directory for data files",
)
@click.option(
    "--formats",
    "-f",
    type=str,
    default="parquet,avro,protobuf",
    help="Comma-separated list of formats (default: all)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducible data",
)
@click.option(
    "--save-results",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to save write results JSON (default: data/write_results.json)",
)
def write(
    size: float,
    output: Path,
    formats: str,
    seed: int | None,
    save_results: Path | None,
):
    """Generate and write data in streaming mode (memory efficient)."""
    click.echo("="*60)
    click.echo("Write Benchmark - Streaming Mode")
    click.echo("="*60)

    # Parse formats
    format_names = [f.strip().lower() for f in formats.split(",")]
    handlers = []

    for format_name in format_names:
        if format_name not in FORMAT_MAP:
            click.echo(f"❌ Unknown format: {format_name}", err=True)
            click.echo(f"Available: {', '.join(FORMAT_MAP.keys())}", err=True)
            return
        handlers.append(FORMAT_MAP[format_name]())

    # Calculate records needed
    generator = OrderGenerator(seed=seed)
    total_records = generator.estimate_records_for_size(size)

    click.echo(f"\n📊 Configuration:")
    click.echo(f"  Target size: {size} GB")
    click.echo(f"  Estimated records: {total_records:,}")
    click.echo(f"  Formats: {', '.join(format_names)}")
    click.echo(f"  Memory usage: ~200MB (streaming)")
    if seed is not None:
        click.echo(f"  Seed: {seed}")

    # Run write benchmark
    wb = WriteBenchmark(output_dir=output)
    results = wb.run_all_formats(handlers, total_records, seed)

    # Save results
    if save_results is None:
        save_results = output / "write_results.json"

    wb.save_results(results, save_results)
    click.echo(f"\n✅ Write benchmark complete!")
    click.echo(f"Next step: uv run dataformat-bench read --input {output}")


@main.command()
@click.option(
    "--input",
    "-i",
    type=click.Path(path_type=Path),
    default=DATA_DIR,
    help="Directory containing data files",
)
@click.option(
    "--formats",
    "-f",
    type=str,
    default="parquet,avro,protobuf",
    help="Comma-separated list of formats (default: all)",
)
@click.option(
    "--runs",
    "-r",
    type=int,
    default=3,
    help="Number of runs for averaging (default: 3)",
)
@click.option(
    "--filter-category",
    type=str,
    default="Electronics",
    help="Category for filtered read tests (default: Electronics)",
)
@click.option(
    "--timeout",
    "-t",
    type=int,
    default=600,
    help="Timeout per operation in seconds (default: 600 = 10min)",
)
@click.option(
    "--save-results",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to save read results JSON (default: data/read_results.json)",
)
def read(
    input: Path,
    formats: str,
    runs: int,
    filter_category: str,
    timeout: int,
    save_results: Path | None,
):
    """Run read benchmark on existing files (memory efficient)."""
    click.echo("="*60)
    click.echo("Read Benchmark")
    click.echo("="*60)

    # Parse formats
    format_names = [f.strip().lower() for f in formats.split(",")]
    handlers = []

    for format_name in format_names:
        if format_name not in FORMAT_MAP:
            click.echo(f"❌ Unknown format: {format_name}", err=True)
            return
        handlers.append(FORMAT_MAP[format_name]())

    click.echo(f"\n📊 Configuration:")
    click.echo(f"  Input directory: {input}")
    click.echo(f"  Formats: {', '.join(format_names)}")
    click.echo(f"  Runs: {runs}")
    click.echo(f"  Filter category: {filter_category}")
    click.echo(f"  Timeout: {timeout}s ({timeout / 60:.1f} min)")

    # Run read benchmark
    rb = ReadBenchmark(
        input_dir=input,
        runs=runs,
        filter_category=filter_category,
        timeout_seconds=timeout,
    )
    results = rb.run_all_formats(handlers)

    # Save results
    if save_results is None:
        save_results = input / "read_results.json"

    rb.save_results(results, save_results)
    click.echo(f"\n✅ Read benchmark complete!")
    click.echo(f"Next step: uv run dataformat-bench report")


@main.command()
@click.option(
    "--write-results",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to write results JSON (default: data/write_results.json)",
)
@click.option(
    "--read-results",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to read results JSON (default: data/read_results.json)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output path for report (default: print to console)",
)
def report(
    write_results: Path | None,
    read_results: Path | None,
    output: Path | None,
):
    """Generate combined report from write and read results."""
    click.echo("="*60)
    click.echo("Generating Report")
    click.echo("="*60)

    # Default paths
    if write_results is None:
        write_results = DATA_DIR / "write_results.json"
    if read_results is None:
        read_results = DATA_DIR / "read_results.json"

    click.echo(f"\nLoading results:")
    click.echo(f"  Write: {write_results}")
    click.echo(f"  Read: {read_results}")

    # Load results
    from .write_benchmark import WriteBenchmark
    from .read_benchmark import ReadBenchmark

    write_data = WriteBenchmark.load_results(write_results)
    read_data = ReadBenchmark.load_results(read_results)

    # Combine results (convert to old BenchmarkResult format for compatibility)
    from .benchmark import BenchmarkResult

    combined_results = []
    for wr in write_data:
        # Find matching read result
        rd = next((r for r in read_data if r.format_name == wr.format_name), None)
        if rd:
            combined_results.append(
                BenchmarkResult(
                    format_name=wr.format_name,
                    file_size_bytes=wr.file_size_bytes,
                    write_time_seconds=wr.write_time_seconds,
                    read_full_time_seconds=rd.read_full_time_seconds,
                    read_filtered_time_seconds=rd.read_filtered_time_seconds,
                    aggregate_time_seconds=rd.aggregate_time_seconds,
                    record_count=wr.record_count,
                )
            )

    # Generate report
    report_gen = ReportGenerator(combined_results)

    if output:
        report_gen.save_report(output)
        report_gen.print_report()
    else:
        report_gen.print_report()

    click.echo("\n✅ Report generated!")


@main.command()
@click.option(
    "--size",
    "-s",
    type=float,
    default=DEFAULT_TARGET_SIZE_GB,
    help=f"Target dataset size in GB (default: {DEFAULT_TARGET_SIZE_GB})",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=DATA_DIR,
    help="Output directory for all files",
)
@click.option(
    "--formats",
    "-f",
    type=str,
    default="parquet,avro,protobuf",
    help="Comma-separated list of formats (default: all)",
)
@click.option(
    "--runs",
    "-r",
    type=int,
    default=3,
    help="Number of read benchmark runs (default: 3)",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducible data",
)
@click.option(
    "--timeout",
    "-t",
    type=int,
    default=600,
    help="Timeout per read operation in seconds (default: 600 = 10min)",
)
@click.option(
    "--report-file",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to save report (default: data/report.md)",
)
def run(
    size: float,
    output: Path,
    formats: str,
    runs: int,
    seed: int | None,
    timeout: int,
    report_file: Path | None,
):
    """Run full pipeline: write -> read -> report."""
    click.echo("="*60)
    click.echo("Full Benchmark Pipeline")
    click.echo("="*60)

    # Parse formats
    format_names = [f.strip().lower() for f in formats.split(",")]
    handlers = []

    for format_name in format_names:
        if format_name not in FORMAT_MAP:
            click.echo(f"❌ Unknown format: {format_name}", err=True)
            return
        handlers.append(FORMAT_MAP[format_name]())

    generator = OrderGenerator(seed=seed)
    total_records = generator.estimate_records_for_size(size)

    click.echo(f"\n📊 Configuration:")
    click.echo(f"  Target size: {size} GB")
    click.echo(f"  Estimated records: {total_records:,}")
    click.echo(f"  Formats: {', '.join(format_names)}")
    click.echo(f"  Read runs: {runs}")
    click.echo(f"  Read timeout: {timeout}s ({timeout / 60:.1f} min)")
    if seed is not None:
        click.echo(f"  Seed: {seed}")

    # Phase 1: Write
    click.echo(f"\n{'='*60}")
    click.echo("Phase 1: Write Benchmark")
    click.echo("="*60)
    wb = WriteBenchmark(output_dir=output)
    write_results = wb.run_all_formats(handlers, total_records, seed)
    wb.save_results(write_results, output / "write_results.json")

    # Phase 2: Read
    click.echo(f"\n{'='*60}")
    click.echo("Phase 2: Read Benchmark")
    click.echo("="*60)
    rb = ReadBenchmark(input_dir=output, runs=runs, timeout_seconds=timeout)
    read_results = rb.run_all_formats(handlers)
    rb.save_results(read_results, output / "read_results.json")

    # Phase 3: Report
    click.echo(f"\n{'='*60}")
    click.echo("Phase 3: Report Generation")
    click.echo("="*60)

    from .benchmark import BenchmarkResult

    combined_results = []
    for wr in write_results:
        rd = next((r for r in read_results if r.format_name == wr.format_name), None)
        if rd:
            combined_results.append(
                BenchmarkResult(
                    format_name=wr.format_name,
                    file_size_bytes=wr.file_size_bytes,
                    write_time_seconds=wr.write_time_seconds,
                    read_full_time_seconds=rd.read_full_time_seconds,
                    read_filtered_time_seconds=rd.read_filtered_time_seconds,
                    aggregate_time_seconds=rd.aggregate_time_seconds,
                    record_count=wr.record_count,
                )
            )

    report_gen = ReportGenerator(combined_results)

    if report_file is None:
        report_file = output / "report.md"

    report_gen.save_report(report_file)
    report_gen.print_report()

    click.echo(f"\n{'='*60}")
    click.echo("✅ Full benchmark complete!")
    click.echo(f"{'='*60}")
    click.echo(f"\nResults saved to:")
    click.echo(f"  - {output / 'write_results.json'}")
    click.echo(f"  - {output / 'read_results.json'}")
    click.echo(f"  - {report_file}")


# Keep old benchmark command for backwards compatibility (deprecated)
@main.command(hidden=True)
def benchmark():
    """[DEPRECATED] Use 'run' command instead."""
    click.echo("⚠️  This command is deprecated. Use 'dataformat-bench run' instead.")
    click.echo("Example: uv run dataformat-bench run --size 10 --runs 3")


@main.command()
@click.option(
    "--records",
    "-n",
    type=int,
    default=1000,
    help="Number of records to generate (default: 1000)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["parquet", "avro", "protobuf"], case_sensitive=False),
    required=True,
    help="Output format",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output file path",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducible data",
)
def generate(records: int, format: str, output: Path, seed: int | None):
    """Generate sample data in specified format."""
    click.echo(f"🎲 Generating {records:,} records in {format.upper()} format...")

    generator = OrderGenerator(seed=seed)
    data = generator.generate_batch(records)

    handler_map = {
        "parquet": ParquetHandler(),
        "avro": AvroHandler(),
        "protobuf": ProtobufHandler(),
    }

    handler = handler_map[format.lower()]
    handler.write(data, output)

    file_size = output.stat().st_size
    click.echo(f"✅ Generated {output}")
    click.echo(f"   Size: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")


if __name__ == "__main__":
    main()
