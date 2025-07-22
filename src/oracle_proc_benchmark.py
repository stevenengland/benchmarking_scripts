#!/usr/bin/env python3
import argparse
import ast
import getpass
import re
import time

import oracledb

from connection.constants import DEFAULT_ORACLEDB_PORT
from measurements.measurement_printing import print_measurement_results
from measurements.measurements_stats import MeasurementsStats
from oracle_db.connection_string import get_connection_string
from oracle_db.measuring import measure_procedure_execution_time
from output.constants import DIVIDE_OP_STR
from output.time_format import format_seconds


def execute_procedure(  # noqa: WPS211
    connection_string: str,
    procedure_name: str,
    procedure_args: list[str],
    count: int,
    wait: float,
    reuse_connection: bool,
    warmup_cache: int,
) -> MeasurementsStats:

    if reuse_connection:
        try:
            with oracledb.connect(
                connection_string,
            ) as connection:
                with connection.cursor() as cursor:
                    return execute_proc_w_reused_cursor(
                        procedure_name=procedure_name,
                        procedure_args=procedure_args,
                        cursor=cursor,
                        count=count,
                        wait=wait,
                        warmup_cache=warmup_cache,
                    )
        except oracledb.DatabaseError as error:
            print(f"Error: {error}")
            exit(1)
    else:
        return execute_proc_wo_reused_cursor(
            connection_string=connection_string,
            procedure_name=procedure_name,
            procedure_args=procedure_args,
            count=count,
            wait=wait,
            warmup_cache=warmup_cache,
        )


def execute_proc_w_reused_cursor(  # noqa: WPS211, WPS210, WPS231
    cursor: oracledb.Cursor,
    procedure_name: str,
    procedure_args: list[str],
    count: int,
    wait: float,
    warmup_cache: int,
) -> MeasurementsStats:
    for warmup_iteration in range(warmup_cache):
        execution_time = measure_procedure_execution_time(
            cursor,
            procedure_name,
            args=procedure_args,
        )
        progress = f"Warmup # {warmup_iteration}{DIVIDE_OP_STR}{warmup_cache}"
        time_and_rows = f"{format_seconds(execution_time)}"
        print(
            f"{progress}: {time_and_rows}",
        )

        time.sleep(wait)

    measurements: list[float] = []
    failed_attempts = 0

    for execution_count in range(1, count + 1):
        try:
            execution_time = measure_procedure_execution_time(
                cursor,
                procedure_name,
                args=procedure_args,
            )
        except Exception as exception:
            attempt_str = f"  Attempt {execution_count}/{count}"
            print(f"{attempt_str}: Error - {exception}")
            failed_attempts += 1
            time.sleep(wait)
        else:
            progress = f"# {execution_count}{DIVIDE_OP_STR}{count}"
            time_and_rows = f"{format_seconds(execution_time)}"
            print(
                f"{progress}: {time_and_rows}",
            )

            measurements.append(execution_time)
            time.sleep(wait)

    return MeasurementsStats(measurements, failed_attempts)


# ToDo: Way to complex function, should be refactored
def execute_proc_wo_reused_cursor(  # noqa: WPS211, WPS210, WPS231, C901
    connection_string: str,
    procedure_name: str,
    procedure_args: list[str],
    count: int,
    wait: float,
    warmup_cache: int,
) -> MeasurementsStats:
    for warmup_iteration in range(warmup_cache):
        try:
            with oracledb.connect(
                connection_string,
            ) as warmup_conn:
                with warmup_conn.cursor() as warmup_cursor:
                    execution_time = measure_procedure_execution_time(
                        warmup_cursor,
                        procedure_name,
                        args=procedure_args,
                    )
                    progress = (
                        f"Warmup # {warmup_iteration}{DIVIDE_OP_STR}{warmup_cache}"
                    )
                    time_and_rows = f"{format_seconds(execution_time)}"
                    print(
                        f"{progress}: {time_and_rows}",
                    )

                    time.sleep(wait)
        except oracledb.DatabaseError as error:
            print(f"Error: {error}")
            exit(1)

    measurements: list[float] = []
    failed_attempts = 0

    for execution_count in range(1, count + 1):
        try:
            with oracledb.connect(
                connection_string,
            ) as measurement_conn:
                with measurement_conn.cursor() as measurement_cursor:
                    execution_time = measure_procedure_execution_time(
                        measurement_cursor,
                        procedure_name,
                        args=procedure_args,
                    )
                    progress = f"# {execution_count}{DIVIDE_OP_STR}{count}"
                    time_and_rows = f"{format_seconds(execution_time)}"
                    print(
                        f"{progress}: {time_and_rows}",
                    )

                    measurements.append(execution_time)
                    time.sleep(wait)
        except oracledb.DatabaseError as measurement_error:
            print(f"Error: {measurement_error}")
            failed_attempts += 1
            time.sleep(wait)

    return MeasurementsStats(measurements, failed_attempts)


def parse_arguments() -> argparse.Namespace:  # noqa: WPS213
    parser = argparse.ArgumentParser(description="Measure procedure exec time")
    parser.add_argument("db_host", type=str, help="Target hostname or IP address")
    parser.add_argument("db_service", type=str, help="Service name of the target db")
    parser.add_argument("db_user", type=str, help="DB username")
    parser.add_argument(
        "procedure",
        type=str,
        help="Name of the procedure to execute with arguments. Example: my_proc(arg1, arg2)",
    )
    parser.add_argument(
        "-p",
        "--db-port",
        type=int,
        default=DEFAULT_ORACLEDB_PORT,
        help=f"The port the DB is listening on (default: {DEFAULT_ORACLEDB_PORT})",
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
        "-r",
        "--reuse-connection",
        action="store_true",
        default=False,
        help="Reuse the connection and cursor for all queries (default: False)?",
    )

    parser.add_argument(
        "-wc",
        "--warmup-cache",
        type=int,
        default=0,
        help="How many times the query(ies) will be executed upfront the real test to warmup caches (default: 0)?",
    )

    parser.add_argument(
        "-etc",
        "--enable-thick-client",
        action="store_true",
        default=False,
        help="Enable thick client for the database connection (default: False)",
    )

    parser.add_argument(
        "-tcd",
        "--thick-client-dir",
        type=str,
        help=("Path to the directory containing the thick client libraries"),
    )

    return parser.parse_args()


def main() -> None:  # noqa: WPS210, WPS213
    args = parse_arguments()

    if args.enable_thick_client:
        if args.thick_client_dir:
            oracledb.init_oracle_client(lib_dir=args.thick_client_dir)
        else:
            oracledb.init_oracle_client()

    db_pass = getpass.getpass("Enter password: ")

    print(
        f"Measuring SQL statement execution for {args.db_host}:{args.db_port}/{args.db_service}",
    )
    print(f"  Timeout: {args.timeout}s")
    print(f"  Count: {args.count}")
    print(f"  Wait: {args.wait}s")
    print(f"  Reuse connection: {args.reuse_connection}")
    print(f"  Warmup cache: {args.warmup_cache}")
    print()

    # Extract procedure name and arguments from args.procedure

    match = re.match(r"(.*)\((.*)\)", args.procedure.replace(" ", ""))
    if not match:
        print("Error: Procedure format should be my_proc or my_proc(arg1, arg2)")
        exit(1)
    procedure_name = match.group(1)
    args_string = match.group(2) or ""
    if args_string:
        try:
            # Wrap in parentheses to make it a valid tuple expression
            parsed_args = ast.literal_eval(f"({args_string})")
        except (ValueError, SyntaxError):
            print(f"Error: Invalid argument format in '{args_string}'")
            exit(1)
        # Handle single argument case (ast.literal_eval returns the value, not a tuple)
        if isinstance(parsed_args, (list, tuple)):
            procedure_args = list(parsed_args)
        else:
            procedure_args = [parsed_args]
    else:
        procedure_args = []

    # You can now use procedure_name and procedure_args as needed

    print(f"Procedure name: {procedure_name}")
    print(f"Procedure arguments: {procedure_args}")

    measurements = execute_procedure(
        connection_string=get_connection_string(
            db_host=args.db_host,
            db_service=args.db_service,
            db_user=args.db_user,
            db_pass=db_pass,
            db_port=args.db_port,
            timeout=args.timeout,
        ),
        procedure_name=procedure_name,
        procedure_args=procedure_args,
        count=args.count,
        wait=args.wait,
        reuse_connection=args.reuse_connection,
        warmup_cache=args.warmup_cache,
    )

    print_measurement_results(measurements)


if __name__ == "__main__":
    main()
