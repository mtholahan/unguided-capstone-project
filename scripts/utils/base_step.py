from scripts.utils.logging import get_logger, write_checkpoint
from scripts.utils.logging import write_metrics

class BaseStep:
    """Base class providing logging and checkpointing for pipeline steps."""

    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(name)

    def run_with_checkpoint(self):
        try:
            self.logger.info(f"Starting {self.name}")
            self.run()  # each subclass implements run()
            write_checkpoint(self.name, "success")
        except Exception as e:
            write_checkpoint(self.name, "failure")
            self.logger.exception(f"Error in {self.name}: {e}")
            raise

    def write_metrics(self, metrics: dict, name: str = None):
        """
        Convenience wrapper for writing step-level metrics to JSON.

        Args:
            metrics (dict): Metrics to save.
            name (str, optional): File name (without extension).
                Defaults to the current step name.
        """
        if name is None:
            name = self.step_name
        write_metrics(metrics, name)
