def get_connection_string(
    db_host: str,
    db_service: str,
    db_user: str,
    db_pass: str,
    db_port: int,
    timeout: float,
) -> str:
    host_port = f"{db_host}:{db_port}"
    credentials = f"{db_user}/{db_pass}"
    timeout_param = f"?transport_connect_timeout={timeout}"
    return f"{credentials}@{host_port}/{db_service}{timeout_param}"
