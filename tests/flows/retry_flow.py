from metaflow import FlowSpec, retry, step, timeout


class RetryFlow(FlowSpec):
    """A flow demonstrating @retry and @timeout decorators."""

    @step
    def start(self):
        self.next(self.process)

    @retry(times=2, minutes_between_retries=0)
    @timeout(seconds=300)
    @step
    def process(self):
        self.value = 42
        self.next(self.end)

    @step
    def end(self):
        print("Value:", self.value)


if __name__ == "__main__":
    RetryFlow()
