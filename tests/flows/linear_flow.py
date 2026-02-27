from metaflow import FlowSpec, step


class LinearFlow(FlowSpec):
    """A simple linear Metaflow flow with three steps."""

    @step
    def start(self):
        self.message = "hello from start"
        self.next(self.process)

    @step
    def process(self):
        self.result = self.message.upper()
        self.next(self.end)

    @step
    def end(self):
        print("Result:", self.result)


if __name__ == "__main__":
    LinearFlow()
