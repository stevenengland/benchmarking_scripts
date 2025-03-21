#!/usr/bin/env python3
import argparse
import getpass
import time

import oracledb

from connection.constants import DEFAULT_ORACLEDB_PORT
from measurements.measurement_printing import print_measurement_results
from measurements.measurements_stats import MeasurementsStats
from oracle_db.connection_string import get_connection_string
from oracle_db.measuring import measure_query_execution_time
from output.constants import DIVIDE_OP_STR
from output.time_format import format_seconds
from sql.sql_file_reader import parse_sql_file


def execute_sql_stmts(  # noqa: WPS211
    connection_string: str,
    queries: list[str],
    count: int,
    wait: float,
    batch_size: int,
    hard_parse: bool,
    reuse_connection: bool,
    warmup_cache: int,
) -> MeasurementsStats:

    if reuse_connection:
        try:
            with oracledb.connect(
                connection_string,
            ) as connection:
                with connection.cursor() as cursor:
                    return execute_sql_stmts_w_reused_cursor(
                        cursor=cursor,
                        queries=queries,
                        count=count,
                        batch_size=batch_size,
                        hard_parse=hard_parse,
                        wait=wait,
                        warmup_cache=warmup_cache,
                    )
        except oracledb.DatabaseError as error:
            print(f"Error: {error}")
            return MeasurementsStats([])
    else:
        return execute_sql_stmts_wo_reused_cursor(
            connection_string=connection_string,
            count=count,
            queries=queries,
            wait=wait,
            batch_size=batch_size,
            hard_parse=hard_parse,
            warmup_cache=warmup_cache,
        )


def execute_sql_stmts_w_reused_cursor(
    cursor: oracledb.Cursor,
    queries: list[str],
    count: int,
    batch_size: int,
    hard_parse: bool,
    wait: float,
    warmup_cache: int,
) -> MeasurementsStats:
    for warmup_iteration in range(warmup_cache):
        affected_rows, execution_time = measure_query_execution_time(
            cursor,
            queries,
            batch_size,
            hard_parse,
        )
        progress = f"Warmup # {warmup_iteration}{DIVIDE_OP_STR}{warmup_cache}"
        time_and_rows = f"{format_seconds(execution_time)}, {affected_rows} rows"
        print(
            f"{progress}: {time_and_rows}",
        )

        time.sleep(wait)

    measurements = []
    failed_attempts = 0

    for execution_count in range(1, count + 1):
        try:
            affected_rows, execution_time = measure_query_execution_time(
                cursor,
                queries,
                batch_size,
                hard_parse,
            )
        except Exception as exception:
            attempt_str = f"  Attempt {execution_count}/{count}"
            print(f"{attempt_str}: Error - {exception}")
            failed_attempts += 1
            time.sleep(wait)
        else:
            progress = f"# {execution_count}{DIVIDE_OP_STR}{count}"
            time_and_rows = f"{format_seconds(execution_time)}, {affected_rows} rows"
            print(
                f"{progress}: {time_and_rows}",
            )

            measurements.append(execution_time)
            time.sleep(wait)

    return MeasurementsStats(measurements, failed_attempts)


# ToDo: Way to complex function, should be refactored
def execute_sql_stmts_wo_reused_cursor(
    connection_string: str,
    queries: list[str],
    count: int,
    wait: float,
    batch_size: int,
    hard_parse: bool,
    warmup_cache: int,
) -> MeasurementsStats:
    for warmup_iteration in range(warmup_cache):
        try:
            with oracledb.connect(
                connection_string,
            ) as warmup_conn:
                with warmup_conn.cursor() as warmup_cursor:
                    affected_rows, execution_time = measure_query_execution_time(
                        warmup_cursor,
                        queries,
                        batch_size,
                        hard_parse,
                    )
                    progress = (
                        f"Warmup # {warmup_iteration}{DIVIDE_OP_STR}{warmup_cache}"
                    )
                    time_and_rows = (
                        f"{format_seconds(execution_time)}, {affected_rows} rows"
                    )
                    print(
                        f"{progress}: {time_and_rows}",
                    )

                    time.sleep(wait)
        except oracledb.DatabaseError as error:
            print(f"Error: {error}")
            return MeasurementsStats([])

    measurements = []
    failed_attempts = 0

    for execution_count in range(1, count + 1):
        try:
            with oracledb.connect(
                connection_string,
            ) as measurement_conn:
                with measurement_conn.cursor() as measurement_cursor:
                    affected_rows, execution_time = measure_query_execution_time(
                        measurement_cursor,
                        queries,
                        batch_size,
                        hard_parse,
                    )
                    progress = f"# {execution_count}{DIVIDE_OP_STR}{count}"
                    time_and_rows = (
                        f"{format_seconds(execution_time)}, {affected_rows} rows"
                    )
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
    parser = argparse.ArgumentParser(description="Measure SQL query time")
    parser.add_argument("db_host", type=str, help="Target hostname or IP address")
    parser.add_argument("db_service", type=str, help="Service name of the target db")
    parser.add_argument("db_user", type=str, help="DB username")
    parser.add_argument(
        "-q",
        "--query",
        type=str,
        default="SELECT SYSDATE FROM DUAL",
        help="The SQL statement to execute (default: SELECT SYSDATE FROM DUAL)",
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
        "-b",
        "--batch-size",
        type=int,
        default=0,
        help="The batch size for fetchmany(). If it is 0 fetchall is called (default: 0)",
    )

    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help=("Path to an file that contains multiple SQL statements to execute"),
    )

    parser.add_argument(
        "-hp",
        "--hard-parse",
        action="store_true",
        default=False,
        help="Force hard parse (default: false)?",
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

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    queries: list[str] = parse_sql_file(args.file) if args.file else [args.query]

    db_pass = getpass.getpass("Enter password: ")

    print(
        f"Measuring SQL statement execution for {args.db_host}:{args.db_port}/{args.db_service}",
    )
    print(f"  Timeout: {args.timeout}s")
    print(f"  Count: {args.count}")
    print(f"  Wait: {args.wait}s")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Hard parse: {args.hard_parse}")
    print(f"  Reuse connection: {args.reuse_connection}")
    print(f"  Warmup cache: {args.warmup_cache}")
    print()

    measurements = execute_sql_stmts(
        connection_string=get_connection_string(
            db_host=args.db_host,
            db_service=args.db_service,
            db_user=args.db_user,
            db_pass=db_pass,
            db_port=args.db_port,
            timeout=args.timeout,
        ),
        queries=queries,
        count=args.count,
        wait=args.wait,
        batch_size=args.batch_size,
        hard_parse=args.hard_parse,
        reuse_connection=args.reuse_connection,
        warmup_cache=args.warmup_cache,
    )

    print_measurement_results(measurements)


if __name__ == "__main__":
    main()
