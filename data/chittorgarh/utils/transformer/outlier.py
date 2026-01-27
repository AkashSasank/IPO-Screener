import json
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Sequence, Tuple


class OutlierPolicy(str, Enum):
    NONE = "none"
    # drop rows outside bounds
    IQR_FILTER = "iqr_filter"
    PCTL_FILTER = "percentile_filter"
    # keep rows but cap values (winsorize)
    PCTL_CLIP = "percentile_clip"
    # first hard-clip plausible bounds, then percentile clip
    CLIP_THEN_PCTL_CLIP = "clip_then_percentile_clip"


@dataclass
class OutlierArtifacts:
    # percentile bounds per column
    pctl_bounds: Dict[str, Tuple[float, float]]

    # IQR bounds per column
    iqr_bounds: Dict[str, Tuple[float, float]]

    # deterministic hard clip bounds
    hard_clip: Dict[str, Tuple[Optional[float], Optional[float]]]

    # ---------- serialization ----------
    def to_json_str(self) -> str:
        return json.dumps(
            {
                "pctl_bounds": {
                    k: [float(v[0]), float(v[1])] for k, v in self.pctl_bounds.items()
                },
                "iqr_bounds": {
                    k: [float(v[0]), float(v[1])] for k, v in self.iqr_bounds.items()
                },
                "hard_clip": {k: [v[0], v[1]] for k, v in self.hard_clip.items()},
            },
            indent=2,
        )

    @staticmethod
    def from_json_str(s: str) -> "OutlierArtifacts":
        d = json.loads(s)

        return OutlierArtifacts(
            pctl_bounds={
                k: (float(v[0]), float(v[1]))
                for k, v in d.get("pctl_bounds", {}).items()
            },
            iqr_bounds={
                k: (float(v[0]), float(v[1]))
                for k, v in d.get("iqr_bounds", {}).items()
            },
            hard_clip={
                k: (
                    float(v[0]) if v[0] is not None else None,
                    float(v[1]) if v[1] is not None else None,
                )
                for k, v in d.get("hard_clip", {}).items()
            },
        )

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json_str())

    @staticmethod
    def load(path: str) -> "OutlierArtifacts":
        with open(path, "r", encoding="utf-8") as f:
            return OutlierArtifacts.from_json_str(f.read())
