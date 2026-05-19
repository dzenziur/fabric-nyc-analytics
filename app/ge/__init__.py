"""Great Expectations runner — orchestrates DQ checks across Silver + Gold."""
from app.ge.runner import run_report

__all__ = ["run_report"]
