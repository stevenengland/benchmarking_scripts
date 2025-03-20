def parse_sql_file(file_path: str) -> list[str]:
    try:
        return _read_and_parse_file(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' was not found.")
    except IOError as error_instance:
        raise IOError(f"Error reading the file: {str(error_instance)}")


def _read_and_parse_file(file_path: str) -> list[str]:
    with open(file_path, "r") as input_file:
        statements_wo_semicolons = input_file.read().split(";")
    sql_statements = []
    for statement in statements_wo_semicolons:
        cleaned_statement = statement.strip()

        if cleaned_statement:
            sql_statements.append(cleaned_statement)

    return sql_statements
