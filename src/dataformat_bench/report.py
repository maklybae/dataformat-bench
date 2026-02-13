"""Report generation for benchmark results."""

from pathlib import Path

from tabulate import tabulate

from .benchmark import BenchmarkResult


class ReportGenerator:
    """Generate formatted reports from benchmark results."""

    def __init__(self, results: list[BenchmarkResult]):
        """Initialize report generator.

        Args:
            results: List of benchmark results
        """
        self.results = sorted(results, key=lambda r: r.format_name)

    def _format_size(self, bytes_value: int) -> str:
        """Format file size in human-readable form.

        Args:
            bytes_value: Size in bytes

        Returns:
            Formatted string (e.g., "2.5 GB")
        """
        gb = bytes_value / (1024**3)
        if gb >= 1:
            return f"{gb:.2f} GB"
        mb = bytes_value / (1024**2)
        if mb >= 1:
            return f"{mb:.2f} MB"
        kb = bytes_value / 1024
        return f"{kb:.2f} KB"

    def _format_time(self, seconds: float | None) -> str:
        """Format time in human-readable form.

        Args:
            seconds: Time in seconds or None if timed out

        Returns:
            Formatted string
        """
        if seconds is None:
            return "TIMEOUT"
        if seconds >= 60:
            minutes = seconds / 60
            return f"{minutes:.2f} min"
        return f"{seconds:.2f} s"

    def _calculate_percentage(self, value: float | None, baseline: float | None) -> str:
        """Calculate percentage difference from baseline.

        Args:
            value: Current value or None if timed out
            baseline: Baseline value or None if timed out

        Returns:
            Formatted percentage string with sign
        """
        if value is None:
            return "TIMEOUT"
        if baseline is None or baseline == 0:
            return "N/A"
        diff_pct = ((value - baseline) / baseline) * 100
        sign = "+" if diff_pct > 0 else ""
        return f"{sign}{diff_pct:.1f}%"

    def generate_summary_table(self) -> str:
        """Generate summary comparison table.

        Returns:
            Formatted table as string
        """
        headers = [
            "Format",
            "File Size",
            "Write Time",
            "Full Scan",
            "Filtered Read",
            "Aggregation",
        ]

        rows = []
        for result in self.results:
            row = [
                result.format_name.upper(),
                self._format_size(result.file_size_bytes),
                self._format_time(result.write_time_seconds),
                self._format_time(result.read_full_time_seconds),
                self._format_time(result.read_filtered_time_seconds),
                self._format_time(result.aggregate_time_seconds),
            ]
            rows.append(row)

        return tabulate(rows, headers=headers, tablefmt="github")

    def generate_comparison_table(self) -> str:
        """Generate detailed comparison table with percentages.

        Returns:
            Formatted comparison table as string
        """
        if not self.results:
            return "No results to compare"

        # Find best (minimum) for each metric, filtering out None values
        best_size = min(r.file_size_bytes for r in self.results)
        best_write = min(r.write_time_seconds for r in self.results)
        
        # For read operations, filter out None (timeout) values
        valid_read_full = [r.read_full_time_seconds for r in self.results if r.read_full_time_seconds is not None]
        best_read_full = min(valid_read_full) if valid_read_full else None
        
        valid_read_filtered = [r.read_filtered_time_seconds for r in self.results if r.read_filtered_time_seconds is not None]
        best_read_filtered = min(valid_read_filtered) if valid_read_filtered else None
        
        valid_aggregate = [r.aggregate_time_seconds for r in self.results if r.aggregate_time_seconds is not None]
        best_aggregate = min(valid_aggregate) if valid_aggregate else None

        headers = [
            "Format",
            "Size vs Best",
            "Write vs Best",
            "Full Scan vs Best",
            "Filtered vs Best",
            "Aggregate vs Best",
        ]

        rows = []
        for result in self.results:
            row = [
                result.format_name.upper(),
                self._calculate_percentage(result.file_size_bytes, best_size),
                self._calculate_percentage(result.write_time_seconds, best_write),
                self._calculate_percentage(result.read_full_time_seconds, best_read_full),
                self._calculate_percentage(
                    result.read_filtered_time_seconds, best_read_filtered
                ),
                self._calculate_percentage(result.aggregate_time_seconds, best_aggregate),
            ]
            rows.append(row)

        return tabulate(rows, headers=headers, tablefmt="github")

    def generate_analysis(self) -> str:
        """Generate analysis and conclusions.

        Returns:
            Analysis text
        """
        if not self.results:
            return "No results to analyze"

        analysis = []
        analysis.append("\n## Analysis\n")

        # File size analysis
        sizes = {r.format_name: r.file_size_bytes for r in self.results}
        best_size_format = min(sizes, key=sizes.get)
        worst_size_format = max(sizes, key=sizes.get)

        analysis.append("### File Size (Storage Efficiency)")
        analysis.append(
            f"- **Best:** {best_size_format.upper()} "
            f"({self._format_size(sizes[best_size_format])})"
        )
        analysis.append(
            f"- **Worst:** {worst_size_format.upper()} "
            f"({self._format_size(sizes[worst_size_format])})"
        )
        compression_ratio = sizes[worst_size_format] / sizes[best_size_format]
        analysis.append(
            f"- {best_size_format.upper()} achieves {compression_ratio:.2f}x "
            f"better compression than {worst_size_format.upper()}"
        )

        # Write performance
        write_times = {r.format_name: r.write_time_seconds for r in self.results}
        best_write_format = min(write_times, key=write_times.get)

        analysis.append("\n### Write Performance")
        analysis.append(
            f"- **Fastest:** {best_write_format.upper()} "
            f"({self._format_time(write_times[best_write_format])})"
        )

        # Read performance
        read_times = {r.format_name: r.read_full_time_seconds for r in self.results if r.read_full_time_seconds is not None}
        
        analysis.append("\n### Full Scan Performance")
        if read_times:
            best_read_format = min(read_times, key=read_times.get)
            analysis.append(
                f"- **Fastest:** {best_read_format.upper()} "
                f"({self._format_time(read_times[best_read_format])})"
            )
        else:
            analysis.append("- All formats timed out during full scan")

        # Filtered read performance
        filtered_times = {r.format_name: r.read_filtered_time_seconds for r in self.results if r.read_filtered_time_seconds is not None}
        
        analysis.append("\n### Filtered Read Performance")
        if filtered_times:
            best_filtered_format = min(filtered_times, key=filtered_times.get)
            analysis.append(
                f"- **Fastest:** {best_filtered_format.upper()} "
                f"({self._format_time(filtered_times[best_filtered_format])})"
            )
        else:
            analysis.append("- All formats timed out during filtered reads")

        # Aggregate performance
        agg_times = {r.format_name: r.aggregate_time_seconds for r in self.results if r.aggregate_time_seconds is not None}
        
        analysis.append("\n### Aggregation Performance")
        if agg_times:
            best_agg_format = min(agg_times, key=agg_times.get)
            analysis.append(
                f"- **Fastest:** {best_agg_format.upper()} "
                f"({self._format_time(agg_times[best_agg_format])})"
            )
        else:
            analysis.append("- All formats timed out during aggregation")

        # Check for predicate pushdown advantage
        for result in self.results:
            if result.format_name == "parquet":
                if result.read_full_time_seconds is not None and result.read_filtered_time_seconds is not None:
                    speedup = (
                        result.read_full_time_seconds / result.read_filtered_time_seconds
                    )
                    if speedup > 2:
                        analysis.append(
                            f"- Parquet shows {speedup:.1f}x speedup with predicate pushdown"
                        )

        return "\n".join(analysis)

    def generate_full_report(self) -> str:
        """Generate complete report with all sections.

        Returns:
            Full report as formatted string
        """
        report_parts = [
            "# Data Format Benchmark Results",
            "",
            f"Total records: {self.results[0].record_count:,}",
            "",
            "## Summary",
            "",
            self.generate_summary_table(),
            "",
            "## Relative Performance",
            "",
            self.generate_comparison_table(),
            "",
            self.generate_analysis(),
        ]

        return "\n".join(report_parts)

    def save_report(self, output_path: Path):
        """Save report to file.

        Args:
            output_path: Path to output file
        """
        report = self.generate_full_report()
        with open(output_path, "w") as f:
            f.write(report)
        print(f"\n📄 Report saved to: {output_path}")

    def print_report(self):
        """Print report to console."""
        print("\n" + self.generate_full_report())
