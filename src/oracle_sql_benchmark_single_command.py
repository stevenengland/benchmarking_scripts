#!/usr/bin/env python3
import argparse
import getpass
import time

import oracledb

from connection.constants import DEFAULT_ORACLEDB_PORT
from output.time_format import format_seconds
from sql.sql_file_reader import parse_sql_file


def execute_query(cursor, query, batch_size) -> tuple[int, float]:
    affected_rows = 0
    start_time = time.perf_counter()
    cursor.execute(query)

    if batch_size > 0:
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            affected_rows = affected_rows + len(rows)
    else:
        affected_rows = len(cursor.fetchall())

    end_time = time.perf_counter()
    execution_time = (end_time - start_time) * 1000
    return affected_rows, execution_time


def execute_sql_stmt(
    db_host: str,
    db_service: str,
    db_user: str,
    db_pass: str,
    query: str,
    db_port,
    timeout,
    batch_size,
    hard_parse,
):
    host_port = f"{db_host}:{db_port}"
    credentials = f"{db_user}/{db_pass}"
    timeout_param = f"?transport_connect_timeout={timeout}"
    connection_string = f"{credentials}@{host_port}/{db_service}{timeout_param}"

    print("Connecting to Oracle database...")
    if hard_parse:
        timestamp = round(time.time() * 1000)
        query = f"{query} /* hard parse interrupt: {timestamp} */ "
    try:
        with oracledb.connect(connection_string) as connection:
            print("Connection established successfully.")
            print()
            with connection.cursor() as cursor:
                affected_rows, execution_time = execute_query(cursor, query, batch_size)
                print(
                    f"Ran in {format_seconds(execution_time)}, got {affected_rows} result(s)",
                )

    except oracledb.Error as error:
        print(f"Database error: {error}")
        return []


def parse_arguments() -> argparse.Namespace:
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
        "-t",
        "--timeout",
        type=float,
        default=2,
        help="Socket timeout in seconds (default: 2)",
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
        help="Force hard parse (default: false)",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    query = parse_sql_file(args.file) if args.file else args.query

    db_pass = getpass.getpass("Enter password: ")

    print(
        f"Measuring SQL statement execution for {args.db_host}:{args.db_port}/{args.db_service}",
    )
    print(f"  Timeout: {args.timeout}s")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Hard parse: {args.hard_parse}")
    print()

    execute_sql_stmt(
        db_host=args.db_host,
        db_service=args.db_service,
        db_user=args.db_user,
        db_pass=db_pass,
        query=query,
        db_port=args.db_port,
        timeout=args.timeout,
        batch_size=args.batch_size,
        hard_parse=args.hard_parse,
    )


if __name__ == "__main__":
    main()
