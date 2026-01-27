import json
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Sequence


# -------------------------
# Strategy Enums
# -------------------------
class ImputerPolicy(str, Enum):
    NONE = "none"
    MEDIAN = "median"
    ZERO = "zero"
    MEDIAN_WITH_MISSING_INDICATOR = "median_with_missing_indicator"
    ZERO_WITH_MISSING_INDICATOR = "zero_with_missing_indicator"


@dataclass
class ImputationArtifacts:
    medians: Dict[str, float]
    add_missing_indicator: List[str]
    zero_fill: List[str]

    # ---------- serialization ----------
    def to_json_str(self) -> str:
        return json.dumps(
            {
                "medians": self.medians,
                "add_missing_indicator": self.add_missing_indicator,
                "zero_fill": self.zero_fill,
            },
            indent=2,
        )

    @staticmethod
    def from_json_str(s: str) -> "ImputationArtifacts":
        d = json.loads(s)
        return ImputationArtifacts(
            medians={k: float(v) for k, v in d.get("medians", {}).items()},
            add_missing_indicator=list(d.get("add_missing_indicator", [])),
            zero_fill=list(d.get("zero_fill", [])),
        )

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json_str())

    @staticmethod
    def load(path: str) -> "ImputationArtifacts":
        with open(path, "r", encoding="utf-8") as f:
            return ImputationArtifacts.from_json_str(f.read())
