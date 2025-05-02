import pandas as pd
from enum import Enum


class Criticity(Enum):
    """Enum for criticity levels."""
    LOW = "Low"
    MEDIUM = "Medium"
    HIGH = "High"
    CRTITICAL = "Critical"
    SEVERE = "Severe"
    MODERATE = "Moderate"
    NONE = "None"


class BaseHeuristic:
    """Base class for reusable configuration evaluations."""

    @staticmethod
    def add_check(checks, category, expected, current, description, criticity):
        checks.append({
            "category": category,
            "expected": expected,
            "current": current if current else "Not presented. Using default.",
            "description": description,
            "criticity": criticity.value if criticity else Criticity.NONE.value
        })

