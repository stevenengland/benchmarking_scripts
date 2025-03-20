from measurements.measurements_stats import MeasurementsStats
from output.time_format import format_seconds


def print_measurement_results(measurements_stats: MeasurementsStats) -> None:
    print("\nResults:")
    print(
        f"  Minimum latency: {format_seconds(measurements_stats.min)}",
    )
    print(
        f"  Maximum latency: {format_seconds(measurements_stats.max)}",
    )
    print(
        f"  Mean latency: {format_seconds(measurements_stats.mean)}",
    )
    print(
        f"  Median latency: {format_seconds(measurements_stats.median)}",
    )
    print(
        f"  Standard deviation: {format_seconds(measurements_stats.stdev)}",
    )
    print(
        f"  Success rate: {measurements_stats.attempts}/{measurements_stats.attempts + measurements_stats.failed_attempts}",  # noqa: E501
    )
