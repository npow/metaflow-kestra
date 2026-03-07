from metaflow import FlowSpec, step, trigger


@trigger(event="my_event")
class TriggerFlow(FlowSpec):
    """A flow with a @trigger event decorator."""

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        print("done")


if __name__ == "__main__":
    TriggerFlow()
