from metaflow import FlowSpec, step


class ForeachFlow(FlowSpec):
    """A flow demonstrating foreach fan-out."""

    @step
    def start(self):
        self.items = [1, 2, 3]
        self.next(self.process, foreach="items")

    @step
    def process(self):
        self.result = self.input * 10
        self.next(self.join)

    @step
    def join(self, inputs):
        self.results = [inp.result for inp in inputs]
        self.next(self.end)

    @step
    def end(self):
        print("Results:", self.results)


if __name__ == "__main__":
    ForeachFlow()
