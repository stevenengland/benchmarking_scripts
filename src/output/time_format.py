from output.constants import TIME_UNIT_STR


def format_seconds(seconds) -> str:
    return f"{seconds:.2f}{TIME_UNIT_STR}"
