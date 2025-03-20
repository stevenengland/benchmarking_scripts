import argparse
import socket
import time

from constants.socket_constants import DEFAULT_HTTP_PORT
from measurements.measurement_printing import print_measurement_results
from measurements.measurements_stats import MeasurementsStats
from output.constants import DIVIDE_OP_STR
from output.time_format import format_seconds


def measure_latency(  # noqa: WPS210, WPS213
    host,
    port,
    count,
    timeout,
    wait,
) -> MeasurementsStats:
    measurements: list[float] = []
    failed_attempts = 0

    for attempt_index in range(count):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_instance:
                socket_instance.settimeout(timeout)

                start_time = time.perf_counter()

                socket_instance.connect((host, port))

                end_time = time.perf_counter()

                socket_instance.close()

                latency = (end_time - start_time) * 1000
                measurements.append(latency)

                attempt_str = f"  Attempt {attempt_index+1}{DIVIDE_OP_STR}{count}"
                latency_str = f"{format_seconds(latency)}"
                print(f"{attempt_str}: {latency_str}")

                time.sleep(wait)

        except socket.timeout:
            print(
                f"  Attempt {attempt_index+1}{DIVIDE_OP_STR}{count}: Timed out after {timeout} seconds",
            )
            failed_attempts += 1

        except Exception as exception:
            attempt_msg = f"  Attempt {attempt_index+1}{DIVIDE_OP_STR}{count}"
            print(f"{attempt_msg}: Error - {exception}")
            failed_attempts += 1
            time.sleep(wait)

    return MeasurementsStats(
        measurements,
        failed_attempts,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure network latency to a host")
    parser.add_argument("host", type=str, help="Target hostname or IP address")
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=DEFAULT_HTTP_PORT,
        help=f"Target port number (default: {DEFAULT_HTTP_PORT})",
    )
    parser.add_argument(
        "-c",
        "--count",
        type=int,
        default=10,
        help="Number of measurements to take (default: 10)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=2,
        help="Socket timeout in seconds (default: 2)",
    )
    parser.add_argument(
        "-w",
        "--wait",
        type=float,
        default=0.5,
        help="Wait time between each attempt (default: 0.5)",
    )

    args = parser.parse_args()

    print(
        f"Measuring socket connection to {args.host}:{args.port}",
    )
    print(f"  Count: {args.count}")
    print(f"  Timeout: {args.timeout}s")
    print(f"  Wait: {args.wait}s")
    print()

    measurement_results = measure_latency(
        args.host,
        args.port,
        args.count,
        args.timeout,
        args.wait,
    )

    print_measurement_results(measurement_results)


if __name__ == "__main__":
    main()
