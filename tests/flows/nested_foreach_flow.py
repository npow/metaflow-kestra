from metaflow import FlowSpec, step


class NestedForeachFlow(FlowSpec):
    """A flow demonstrating nested foreach (foreach inside foreach)."""

    @step
    def start(self):
        self.outer_items = ["a", "b"]
        self.next(self.outer, foreach="outer_items")

    @step
    def outer(self):
        # Each outer task fans out over inner items
        self.inner_items = [1, 2, 3]
        self.next(self.inner, foreach="inner_items")

    @step
    def inner(self):
        self.result = "%s-%d" % (self.input, self.input)
        self.next(self.inner_join)

    @step
    def inner_join(self, inputs):
        self.inner_results = [inp.result for inp in inputs]
        self.next(self.outer_join)

    @step
    def outer_join(self, inputs):
        self.all_results = [r for inp in inputs for r in inp.inner_results]
        self.next(self.end)

    @step
    def end(self):
        print("All results:", self.all_results)


if __name__ == "__main__":
    NestedForeachFlow()
