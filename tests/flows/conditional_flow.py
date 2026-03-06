from metaflow import FlowSpec, Parameter, step


class ConditionalFlow(FlowSpec):
    """A flow with conditional branching via self.next({...}, condition=...).

    Only one branch (high_branch or low_branch) executes at runtime.
    The 'after' step is a regular linear step that receives the output
    of whichever branch was taken.
    """

    value = Parameter("value", default=42, type=int)

    @step
    def start(self):
        # Set the condition variable; Metaflow routes to the matching branch.
        self.route = "high" if self.value >= 50 else "low"
        self.next(
            {"high": self.high_branch, "low": self.low_branch},
            condition="route",
        )

    @step
    def high_branch(self):
        self.label = "high"
        self.doubled = self.value * 2
        self.next(self.after)

    @step
    def low_branch(self):
        self.label = "low"
        self.doubled = self.value * 2
        self.next(self.after)

    @step
    def after(self):
        # Receives output from whichever branch ran (not a join — no 'inputs' arg)
        print("Branch taken:", self.label, "doubled:", self.doubled)
        self.next(self.end)

    @step
    def end(self):
        print("Done. label=%s doubled=%s" % (self.label, self.doubled))


if __name__ == "__main__":
    ConditionalFlow()
