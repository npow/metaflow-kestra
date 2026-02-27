from metaflow import FlowSpec, schedule, step


@schedule(cron="0 * * * *")
class ScheduleFlow(FlowSpec):
    """A flow with an hourly schedule trigger."""

    @step
    def start(self):
        self.message = "scheduled run"
        self.next(self.end)

    @step
    def end(self):
        print(self.message)


if __name__ == "__main__":
    ScheduleFlow()
