"""Quick test to confirm BaseStep live console logging in PowerShell 7."""

from base_step import BaseStep
import time

class StepLoggingTest(BaseStep):
    def __init__(self):
        super().__init__("LoggingTest")

    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting BaseStep logging test...")

        # simulate work
        for i in self.progress_iter(range(5), desc="Simulated Work", unit="item"):
            self.logger.info(f"Working on iteration {i+1}/5...")
            time.sleep(0.8)

        self.logger.warning("⚠️ This is a test warning (visible immediately).")
        self.logger.info("✅ Logging test completed successfully.")


if __name__ == "__main__":
    StepLoggingTest().run()
