#!/usr/bin/env python3
import argparse
import oracledb
import time
import statistics
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

        return sql_statements

    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except IOError as e:
        raise IOError(f"Error reading the file: {str(e)}")


def run_benchmark_with_list_of_sql_statements(
    db_host: str,
    db_service: str,
    db_user: str,
    db_pass: str,
    queries: list[str],
    db_port=1521,
    count=10,
    timeout=2,
    wait=0.5,
    batch_size=0,
):
    results = []
    connection_string = f"{db_user}/{db_pass}@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}"

    print(
        f"Connecting to Oracle database via {db_user}/******@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}"
    )
    try:
        with oracledb.connect(connection_string) as connection:

            print("Connection established successfully.")
            print(
                f"Will execute the queries {count} times with {wait} second(s) between runs and fetch size of {batch_size} (0 = all)."
            )
            print()

            with connection.cursor() as cursor:
                # Run the query once before the test to warm up caches
                start_time_initial_query = time.perf_counter()
                for query in queries:
                    cursor.execute(query)

                    if batch_size > 0:
                        while True:
                            rows = cursor.fetchmany(batch_size)
                            if not rows:
                                break
                    else:
                        cursor.fetchall()
                end_time_initial_query = time.perf_counter()
                execution_time_initial_query = (
                    end_time_initial_query - start_time_initial_query
                ) * 1000
                results.append(execution_time_initial_query)
                print(
                    f"Warm up execution 1: {format_time(execution_time_initial_query)} ms"
                )
                # Two more executions so that cursur cache comes in
                cursor.execute(query)

                if batch_size > 0:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                else:
                    cursor.fetchall()
                print(f"Warm up execution 2 done...")
                cursor.execute(query)

                if batch_size > 0:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                else:
                    cursor.fetchall()
                print(f"Warm up execution 3 done...")

                for i in range(1, count + 1):
                    start_time = time.perf_counter()

                    for query in queries:
                        cursor.execute(query)

                        if batch_size > 0:
                            while True:
                                rows = cursor.fetchmany(batch_size)
                                if not rows:
                                    break
                        else:
                            cursor.fetchall()

                    end_time = time.perf_counter()

                    execution_time = (end_time - start_time) * 1000
                    print(f"Ran iteration {i} of {count} in {execution_time} ms ...")
                    results.append(execution_time)

                    if i < count:
                        time.sleep(wait)

    except oracledb.Error as error:
        print(f"Database error: {error}")
        return []

    return results


def run_benchmark_with_same_sql_multiple_times(
    db_host: str,
    db_service: str,
    db_user: str,
    db_pass: str,
    query: str,
    db_port=1521,
    count=10,
    timeout=2,
    wait=0.5,
    batch_size=0,
):
    results = []
    connection_string = f"{db_user}/{db_pass}@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}"

    print(
        f"Connecting to Oracle database via {db_user}/******@{db_host}:{db_port}/{db_service}?transport_connect_timeout={timeout}"
    )
    try:
        with oracledb.connect(connection_string) as connection:

            print("Connection established successfully.")
            print(
                f"Will execute the queries {count} times with {wait} second(s) between runs and fetch size of {batch_size} (0 = all)."
            )
            print()

            with connection.cursor() as cursor:
                # Run the query once before the test to warm up caches
                start_time_initial_query = time.perf_counter()
                cursor.execute(query)

                if batch_size > 0:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                else:
                    cursor.fetchall()
                end_time_initial_query = time.perf_counter()
                execution_time_initial_query = (
                    end_time_initial_query - start_time_initial_query
                ) * 1000
                results.append(execution_time_initial_query)
                print(
                    f"Warm up execution 1: {format_time(execution_time_initial_query)} ms"
                )
                # Two more executions so that session cursur cache comes in
                cursor.execute(query)

                if batch_size > 0:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                else:
                    cursor.fetchall()
                print(f"Warm up execution 2 done...")
                cursor.execute(query)

                if batch_size > 0:
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                else:
                    cursor.fetchall()
                print(f"Warm up execution 3 done...")
                print("Beginning measurement...")

                for i in range(1, count + 1):
                    start_time = time.perf_counter()

                    cursor.execute(query)

                    if batch_size > 0:
                        while True:
                            rows = cursor.fetchmany(batch_size)
                            if not rows:
                                break
                    else:
                        cursor.fetchall()

                    end_time = time.perf_counter()

                    execution_time = (end_time - start_time) * 1000
                    print(f"Ran iteration {i} of {count} in {execution_time} ms ...")
                    results.append(execution_time)

                    if i < count:
                        time.sleep(wait)

    except oracledb.Error as error:
        print(f"Database error: {error}")
        return []

    return results


def print_results(results):
    if not results:
        print("No results gathered.")
        return

    min_time = min(results)
    max_time = max(results)
    avg_time = statistics.mean(results)
    med_time = statistics.median(results)

    print("\nSummary Statistics:")
    print("----------------------------")
    print(f"Minimum: {format_time(min_time)} ms")
    print(f"Maximum: {format_time(max_time)} ms")
    print(f"Mean: {format_time(avg_time)} ms")
    print(f"Median: {format_time(med_time)} ms")


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

    args = parser.parse_args()

    print("Starting Oracle query benchmark...")
    db_pass = getpass.getpass("Enter password: ")
    if args.file:
        queries = parse_sql_file(args.file)
        results = run_benchmark_with_list_of_sql_statements(
            queries=queries,
            db_host=args.db_host,
            db_service=args.db_service,
            db_user=args.db_user,
            db_pass=db_pass,
            db_port=args.db_port,
            count=args.count,
            timeout=args.timeout,
            wait=args.wait,
            batch_size=args.batch_size,
        )
    else:
        results = run_benchmark_with_same_sql_multiple_times(
            db_host=args.db_host,
            db_service=args.db_service,
            db_user=args.db_user,
            db_pass=db_pass,
            query=args.query,
            db_port=args.db_port,
            count=args.count,
            timeout=args.timeout,
            wait=args.wait,
            batch_size=args.batch_size,
        )
    if results:
        print(f"\nExecution time of warm up: {format_time(results[0])} ms")
        print_results(results[1:])


if __name__ == "__main__":
    main()
