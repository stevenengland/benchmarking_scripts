import time

import oracledb


def convert_to_hard_parse_statemtent(query: str) -> str:
    timestamp = round(time.time() * 1000)
    return f"{query} /* hard parse interrupt: {timestamp} */ "


def measure_query_execution_time(
    cursor: oracledb.Cursor,
    queries: list[str],
    batch_size: int,
    hard_parse: bool,
) -> tuple[int, float]:
    for query in queries:
        if hard_parse:
            query = convert_to_hard_parse_statemtent(query)
        if batch_size == 0:
            affected_rows, execution_time = measure_fetchall(cursor, query)
        else:
            affected_rows, execution_time = measure_fetchmany(cursor, query, batch_size)
    return affected_rows, execution_time


def measure_fetchall(cursor: oracledb.Cursor, query: str) -> tuple[int, float]:
    start_time = time.perf_counter()
    cursor.execute(query)
    rowscount = cursor.fetchall()
    end_time = time.perf_counter()
    return len(rowscount), (end_time - start_time) * 1000


def measure_fetchmany(
    cursor: oracledb.Cursor,
    query: str,
    batch_size: int,
) -> tuple[int, float]:
    start_time = time.perf_counter()
    cursor.execute(query)
    affected_rows = 0
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        affected_rows = affected_rows + len(rows)
    end_time = time.perf_counter()
    return affected_rows, (end_time - start_time) * 1000
