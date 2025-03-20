#!/usr/bin/env python3
import argparse

import oracledb
import time
import getpass


def format_time(seconds):
    return f"{seconds:.2f}"


def parse_sql_file(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read()
        statements_with_semicolons = content.split(";")
        sql_statements = []
        for statement in statements_with_semicolons:
            cleaned_statement = statement.strip()

            if cleaned_statement:
                sql_statements.append(cleaned_statement)

        return sql_statements[0]

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except IOError as e:
        raise IOError(f"Error reading the file: {str(e)}")


def run_single_sql(
    db_host: str,
    db_service: str,
    db_user: str,
    db_pass: str,
    query: str,
    db_port=1521,
    timeout=2,
    batch_size=0,
    hard_parse=False,
):
    connection_string = f"{db_user}/{db_pass}@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}"

    print(
        f"Connecting to Oracle database via {db_user}/******@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}, using hard parse: {hard_parse}"
    )
    print("Connection established successfully.")
    print()
    if hard_parse:
        query = query + f" /* hard parse interrupt: {round(time.time()*1000)} */ "
    try:
        with oracledb.connect(connection_string) as connection:
            with connection.cursor() as cursor:
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
                print(
                    f"Ran in {format_time(execution_time)} ms ..., got {affected_rows} results"
                )

    except oracledb.Error as error:
        print(f"Database error: {error}")
        return []


def main():
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
        default=1521,
        help="The port the DB is listening on (default: 1521)",
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

    args = parser.parse_args()

    query = parse_sql_file(args.file) if args.file else args.query

    print("Starting Oracle query benchmark...")
    db_pass = getpass.getpass("Enter password: ")
    run_single_sql(
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
