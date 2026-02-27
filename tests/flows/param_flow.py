from metaflow import FlowSpec, Parameter, step


class ParamFlow(FlowSpec):
    """A flow with parameters."""

    greeting = Parameter("greeting", default="hello", help="Greeting message")
    count = Parameter("count", default=3, type=int, help="Number of repetitions")

    @step
    def start(self):
        self.message = self.greeting * self.count
        self.next(self.end)

    @step
    def end(self):
        print("Message:", self.message)


if __name__ == "__main__":
    ParamFlow()
