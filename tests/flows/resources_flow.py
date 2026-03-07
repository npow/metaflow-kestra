from metaflow import FlowSpec, resources, step


class ResourcesFlow(FlowSpec):
    """A flow with @resources decorators on steps."""

    @step
    def start(self):
        self.next(self.heavy)

    @resources(cpu=4, memory=8192, gpu=1)
    @step
    def heavy(self):
        print("heavy step")
        self.next(self.end)

    @step
    def end(self):
        print("done")


if __name__ == "__main__":
    ResourcesFlow()
