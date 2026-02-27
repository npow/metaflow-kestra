from metaflow import FlowSpec, step


class BranchFlow(FlowSpec):
    """A flow with a split/join (branching) structure."""

    @step
    def start(self):
        self.data = [1, 2, 3, 4, 5]
        self.next(self.branch_a, self.branch_b)

    @step
    def branch_a(self):
        self.result_a = sum(self.data)
        self.next(self.join)

    @step
    def branch_b(self):
        self.result_b = max(self.data)
        self.next(self.join)

    @step
    def join(self, inputs):
        self.total = inputs.branch_a.result_a
        self.maximum = inputs.branch_b.result_b
        self.next(self.end)

    @step
    def end(self):
        print("Sum:", self.total, "Max:", self.maximum)


if __name__ == "__main__":
    BranchFlow()
