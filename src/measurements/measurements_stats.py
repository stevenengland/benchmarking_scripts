import statistics


class MeasurementsStats:  # noqa: WPS230
    def __init__(self, measurements: list[float], failed_attempts: int = 0) -> None:
        self.latencies = measurements
        self.min = min(self.latencies)
        self.max = max(self.latencies)
        self.mean = statistics.mean(self.latencies)
        self.median = statistics.median(self.latencies)
        self.stdev: float = 0
        if len(self.latencies) > 1:
            self.stdev = statistics.stdev(self.latencies)

        self.failed_attempts = failed_attempts
        self.attempts = len(self.latencies) + failed_attempts
