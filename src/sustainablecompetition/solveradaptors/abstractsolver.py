"""Abstract Solver Adaptor"""

from abc import ABC, abstractmethod


class AbstractSolverAdaptor(ABC):
    """Interface for Solver Adaptors"""

    @abstractmethod
    def get_path(self, solver_id: str) -> str:
        """Return the path to the solver file."""
        raise NotImplementedError("Subclasses must implement get_solver_path()")
