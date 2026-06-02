"""Abstract base class for all stress test scenarios."""

import abc

from stress_tests.metrics import ScenarioResult


class BaseScenario(abc.ABC):
    name: str = "unnamed"
    requires_docker: bool = False

    def setup(self) -> None:
        """Called once before run(). Override to allocate resources."""

    @abc.abstractmethod
    def run(self) -> ScenarioResult:
        """Execute the scenario and return measured results."""

    def teardown(self) -> None:
        """Called once after run(), even on failure. Override to release resources."""
