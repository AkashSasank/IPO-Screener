import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Dict


class NormalizationPolicy(str, Enum):
    NONE = "none"
    LOG1P = "log1p"  # good for heavy-tailed AMOUNT-like columns
    ROBUST_Z = "robust_z"  # good default for ratios/multiples
    MINMAX = "minmax"


# =========================
# Normalization artifacts (what we save from training to reuse at inference)
# =========================
@dataclass
class NormalizationArtifacts:
    # For LOG1P: per-column shift added before log1p (0 if not needed)
    log1p_shift: Dict[str, float]

    # For ROBUST_Z: per-column median and iqr
    robust_median: Dict[str, float]
    robust_iqr: Dict[str, float]

    # For MINMAX: per-column min and max
    minmax_min: Dict[str, float]
    minmax_max: Dict[str, float]

    def to_json_str(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, indent=2)

    @staticmethod
    def from_json_str(s: str) -> "NormalizationArtifacts":
        d = json.loads(s)
        return NormalizationArtifacts(
            log1p_shift={k: float(v) for k, v in d.get("log1p_shift", {}).items()},
            robust_median={k: float(v) for k, v in d.get("robust_median", {}).items()},
            robust_iqr={k: float(v) for k, v in d.get("robust_iqr", {}).items()},
            minmax_min={k: float(v) for k, v in d.get("minmax_min", {}).items()},
            minmax_max={k: float(v) for k, v in d.get("minmax_max", {}).items()},
        )

    def save(self, path: str) -> None:
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json_str())

    @staticmethod
    def load(path: str) -> "NormalizationArtifacts":
        with open(path, "r", encoding="utf-8") as f:
            return NormalizationArtifacts.from_json_str(f.read())
