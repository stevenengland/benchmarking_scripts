import argparse
import re
import socket
import time

from constants.socket_constants import (
    DEFAULT_ORACLEDB_PORT,
    DEFAULT_RECEIVE_BUFFER_SIZE,
)
from measurements.measurement_printing import print_measurement_results
from measurements.measurements_stats import MeasurementsStats
from output.time_format import format_seconds

packet = (
    b"\x00W\x00\x00\x01\x00\x00\x00\x018\x01,\x00\x00\x08\x00\x7f\xff"
    + b"\x7f\x08\x00\x00\x01\x00\x00\x1d\x00:\x00\x00\x00\x00\x00\x00"  # noqa: W503
    + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x190\x00\x00\x00\x8d"  # noqa: W503
    + b"\x00\x00\x00\x00\x00\x00\x00\x00(CONNECT_DATA=(COMMAND=ping))"  # noqa: W503
)

pattern = r"\(DESCRIPTION=\(TMP=\)\(VSNNUM=0\)\(ERR=0\)\(ALIAS=.*?\)\)"


def measure_single_tns_ping(
    host: str,
    port: int,
    timeout: float,
    include_conn_setup: bool,
) -> float:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as tcp_socket:
        tcp_socket.settimeout(timeout)
        received = "no data"

        if include_conn_setup:
            start_time = time.perf_counter()
        tcp_socket.connect((host, port))
        if not include_conn_setup:
            start_time = time.perf_counter()

        tcp_socket.send(packet)
        while True:
            received_data = tcp_socket.recv(DEFAULT_RECEIVE_BUFFER_SIZE)
            if not received_data:
                break
            received = received_data[12:].decode()  # noqa: WPS432

        end_time = time.perf_counter()
        tcp_socket.close()

        if not re.match(pattern, received):
            raise ValueError(f"Wrong TNSPing answer received: {received}")

        return (end_time - start_time) * 1000


def measure_tns_pings(
    host,
    port,
    count,
    timeout,
    wait,
    include_conn_setup=False,
) -> MeasurementsStats:
    measurements = []
    failed_attempts = 0

    for attempt_index in range(count):
        try:
            measurement = measure_single_tns_ping(
                host,
                port,
                timeout,
                include_conn_setup,
            )
        except socket.timeout:
            print(
                f"  Attempt {attempt_index+1}/{count}: Timed out after {timeout} seconds",
            )
            failed_attempts += 1

        except Exception as exception:
            attempt_str = f"  Attempt {attempt_index+1}/{count}"
            print(f"{attempt_str}: Error - {exception}")
            failed_attempts += 1
            time.sleep(wait)

        else:
            measurements.append(measurement)
            attempt_str = f"  Attempt {attempt_index+1}/{count}"
            latency_str = f"{format_seconds(measurement)}."
            print(f"{attempt_str}: {latency_str}")
            time.sleep(wait)

    if measurements:
        return MeasurementsStats(measurements)
    return MeasurementsStats([])


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure network latency to a host")
    parser.add_argument("host", type=str, help="Target hostname or IP address")
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=DEFAULT_ORACLEDB_PORT,
        help=f"Target port number (default: {DEFAULT_ORACLEDB_PORT})",
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
    parser.add_argument(
        "-i",
        "--include-conn-setup",
        action="store_true",
        default=False,
        help="Include the connection setup before sending the ping into the measurement? (default: False)",
    )

    args = parser.parse_args()

    print(f"Measuring TNS Ping towards {args.host}:{args.port}")
    print(f"  Count: {args.count}")
    print(f"  Timeout: {args.timeout}s")
    print(f"  Wait: {args.wait}s")
    print(f"  Include connection setup: {args.include_conn_setup}")
    print()

    measurements = measure_tns_pings(
        args.host,
        args.port,
        args.count,
        args.timeout,
        args.wait,
        args.include_conn_setup,
    )

    print_measurement_results(measurements)


if __name__ == "__main__":
    main()
