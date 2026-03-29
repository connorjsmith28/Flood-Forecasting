import json
from pathlib import Path

import numpy as np


_DEFAULT_THRESHOLDS = Path(__file__).resolve().parents[1] / "static" / "top_site_quantile_thesholds.json"

# Ordered from least to most severe
_RETURN_PERIODS = ["Q2_cfs", "Q5_cfs", "Q10_cfs", "Q25_cfs", "Q50_cfs", "Q100_cfs"]


class FloodClassifier:
    """Classify model predictions as flood events using per-site return-period thresholds.

    Args:
        thresholds_path: Path to a JSON file mapping site_id -> {Q2_cfs, Q5_cfs, ...}.
                         Defaults to src/static/top_site_quantile_thesholds.json.
    """

    def __init__(
        self,
        thresholds_path: str | Path | None = None,
    ) -> None:
        path = Path(thresholds_path) if thresholds_path is not None else _DEFAULT_THRESHOLDS
        with open(path) as f:
            self.thresholds: dict[str, dict[str, float]] = json.load(f)

    def classify(
        self,
        predictions: np.ndarray,
        site_ids: np.ndarray,
    ) -> list[dict[str, bool]]:
        """Classify each prediction against all return-period thresholds.

        Args:
            predictions: Unscaled streamflow predictions in CFS, shape (n,).
            site_ids: Site ID strings aligned with predictions, shape (n,).

        Returns:
            List of dicts, one per prediction. Each dict maps each return period
            label to a boolean indicating whether that threshold was exceeded.
            Example: {"Q2_cfs": True, "Q5_cfs": True, "Q10_cfs": False, ...}
            All values are False when the site has no threshold data.
        """
        predictions = np.asarray(predictions).ravel()
        site_ids = np.asarray(site_ids).ravel()
        results = []

        for pred, site in zip(predictions, site_ids):
            site_thresholds = self.thresholds.get(str(site))
            if site_thresholds is None:
                results.append({rp: False for rp in _RETURN_PERIODS})
            else:
                results.append({rp: bool(pred >= site_thresholds[rp]) for rp in _RETURN_PERIODS})

        return results
