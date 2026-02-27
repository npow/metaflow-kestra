from metaflow import FlowSpec, project, step


@project(name="myteam")
class ProjectFlow(FlowSpec):
    """A flow demonstrating @project namespace isolation."""

    @step
    def start(self):
        self.value = 100
        self.next(self.end)

    @step
    def end(self):
        print("Value:", self.value)


if __name__ == "__main__":
    ProjectFlow()
