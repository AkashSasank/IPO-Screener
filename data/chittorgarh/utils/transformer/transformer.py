import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

import dask.dataframe as dd
import numpy as np
import pandas as pd
from chittorgarh.utils.transformer.columns import IPOColumn
from chittorgarh.utils.transformer.imputer import (ImputationArtifacts,
                                                   ImputerPolicy)
from chittorgarh.utils.transformer.normalizer import (NormalizationArtifacts,
                                                      NormalizationPolicy)
from chittorgarh.utils.transformer.outlier import (OutlierArtifacts,
                                                   OutlierPolicy)
from chittorgarh.utils.transformer.strategy import ColumnStrategy

from data.utils.config import parse_config

ColumnLike = Union[str, IPOColumn]


class DataTransformer:
    def __init__(self, config_path: str):
        self.config = parse_config(config_path)
        self.common_field = "company"
        # Dataset is categorised to three layers: silver, gold, bronze
        # bronze: Combined raw data
        # silver: Cleaned data after combining
        # gold: Feature engineered data
        self.bronze: dict = {}
        self.silver: str = None
        self.gold: dict = {}

        self.strategy_map: Dict[str, ColumnStrategy] = IPOColumn.strategy_map()

    def create_bronze(self):
        dataset_root = self.config["dataset_root"]
        segments = self.config["segments"]
        out_files = []

        output_root = os.path.join(dataset_root, "processed", "csv", "bronze")
        os.makedirs(output_root, exist_ok=True)
        for segment in segments:
            input_path = os.path.join(
                dataset_root,
                "processed",
                "csv",
                segment,
            )

            output_file = os.path.join(output_root, f"{segment}.csv")
            self.bronze[segment] = output_file
            out_files.append(output_file)
            files = sorted(os.listdir(input_path))
            df = dd.read_csv(os.path.join(input_path, files[0]))
            for file in files[1:]:
                file_path = os.path.join(input_path, file)
                print(f"Combining data from {file_path} into {output_file}")
                df = df.merge(dd.read_csv(file_path), on=self.common_field, how="left")
            df.drop_duplicates(subset=self.common_field, inplace=True)
            df.to_csv(output_file, index=False, single_file=True)
            print(f"Combined data saved to {output_file}")

        print("Combining segments...")
        output_file = os.path.join(
            dataset_root, "processed", "csv", "bronze", "combined.csv"
        )
        self.bronze["combined"] = output_file
        df = dd.concat([dd.read_csv(file) for file in out_files], axis=0)
        df.drop_duplicates(subset=[self.common_field, "open_date"], inplace=True)
        df.to_csv(output_file, index=False, single_file=True)
        print(f"Combined data saved to {output_file}")

    def create_silver(self):
        if not self.bronze.get("combined"):
            self.create_bronze()
        combined_df = self.combined()
        # Perform column renaming and dropping to resolve conflicts
        # These steps are determined during EDA
        drop_columns = ["issue_price_x", "pe_multiple_x"]
        df = combined_df.drop(columns=drop_columns)
        rename_columns = {
            "issue_price_y": "issue_price",
            "pe_multiple_y": "pe_multiple",
        }
        df = df.rename(columns=rename_columns)

        dataset_root = self.config["dataset_root"]
        output_root = os.path.join(dataset_root, "processed", "csv", "silver")
        os.makedirs(output_root, exist_ok=True)
        output_file = os.path.join(output_root, "data.csv")
        df.to_csv(output_file, index=False, single_file=True)
        self.silver = output_file
        print(f"Silver data saved to {output_file}")

    def create_gold(self, columns: List[IPOColumn], dataset_name: Optional[str] = None):
        if self.silver is None:
            self.create_silver()
        silver_df = dd.read_csv(self.silver)
        silver_df = silver_df.reset_index(drop=True)

        dataset_root = self.config["dataset_root"]
        output_root = os.path.join(dataset_root, "processed", "csv", "gold")
        os.makedirs(output_root, exist_ok=True)
        if not dataset_name:
            dataset_name = f"data_{datetime.today().strftime('%Y%m%d')}"
        output_file = os.path.join(output_root, dataset_name + ".csv")

        print("Fitting imputer...")
        imputer_art = self.fit_imputer(silver_df, columns)
        imputer_art.save(os.path.join(output_root, dataset_name + "_imputer.json"))
        print("Fitting outlier...")
        outlier_art = self.fit_outliers(silver_df, columns)
        outlier_art.save(os.path.join(output_root, dataset_name + "_outlier.json"))
        print("Fitting normalizer...")
        normalizer = self.fit_normalizer(silver_df, columns)
        normalizer.save(os.path.join(output_root, dataset_name + "_normalizer.json"))
        df1 = silver_df[self.select(silver_df, columns)]
        df2 = self.impute(imputer_art, df1, columns)
        df3 = self.handle_outliers(outlier_art, df2, columns, mode="clip")
        gold_df = self.normalize(normalizer, df3, columns)
        gold_df.to_csv(output_file, index=False, single_file=True)
        self.gold[dataset_name] = output_file
        print(f"Gold data saved to {output_file}")

    def combined(
        self,
    ) -> dd.DataFrame:
        return dd.read_csv(self.bronze["combined"])

    def sme(self) -> dd.DataFrame:
        return dd.read_csv(self.bronze["sme"])

    def mainboard(self) -> dd.DataFrame:
        return dd.read_csv(self.bronze["mainboard"])

    #########################################################################

    @staticmethod
    def to_col_names(cols: Optional[Sequence[ColumnLike]]) -> Optional[List[str]]:
        if cols is None:
            return None
        out: List[str] = []
        for c in cols:
            out.append(c if isinstance(c, str) else c.col)
        return out

    def existing(self, df: dd.DataFrame, cols: Iterable[str]) -> List[str]:
        return [c for c in cols if c in df.columns]

    def get_strategy(self, col: str) -> Optional["ColumnStrategy"]:
        return self.strategy_map.get(col)

    def ensure_numeric(self, df: dd.DataFrame, cols: Sequence[str]) -> None:
        for col in cols:
            if col in df.columns:
                df[col] = dd.to_numeric(df[col], errors="coerce")

    def select(
        self, df: dd.DataFrame, cols: Optional[Sequence[ColumnLike]]
    ) -> List[str]:
        if cols is None:
            # all known strategy columns that exist in df
            return [c for c in self.strategy_map.keys() if c in df.columns]
        names = self.to_col_names(cols) or []
        return self.existing(df, names)

    def group_cols_by_policy(
        self, df: dd.DataFrame, cols: Sequence[ColumnLike]
    ) -> Dict[str, List[str]]:
        """
        Returns {policy_name: [colnames...]} using ColumnStrategy.
        """
        target_cols = self.select(df, cols)
        grouped: Dict[str, List[str]] = {}
        for c in target_cols:
            strat = self.get_strategy(c)
            if strat is None:
                continue
            grouped.setdefault(strat.imputer.value, []).append(c)
        return grouped

    #########################################################################
    def fit_normalizer(
        self,
        df: Optional[dd.DataFrame] = None,
        cols: Optional[Sequence[ColumnLike]] = None,
    ) -> NormalizationArtifacts:
        """
        Learns normalization parameters from df (training data).
        """
        target_cols = self.select(df, cols)

        # split by normalization policy
        log_cols: List[str] = []
        robust_cols: List[str] = []
        minmax_cols: List[str] = []

        for col in target_cols:
            strat = self.get_strategy(col)
            if strat is None:
                continue
            if strat.normalization == NormalizationPolicy.LOG1P:
                log_cols.append(col)
            elif strat.normalization == NormalizationPolicy.ROBUST_Z:
                robust_cols.append(col)
            elif strat.normalization == NormalizationPolicy.MINMAX:
                minmax_cols.append(col)

        # ensure numeric
        self.ensure_numeric(df, list(set(log_cols + robust_cols + minmax_cols)))

        log_cols = self.existing(df, log_cols)
        robust_cols = self.existing(df, robust_cols)
        minmax_cols = self.existing(df, minmax_cols)

        log1p_shift: Dict[str, float] = {}
        robust_median: Dict[str, float] = {}
        robust_iqr: Dict[str, float] = {}
        minmax_min: Dict[str, float] = {}
        minmax_max: Dict[str, float] = {}

        # LOG1P shifts: compute mins once
        if log_cols:
            mins = df[log_cols].min().compute()
            for col in log_cols:
                mn = float(mins[col])
                # log1p requires x > -1
                if np.isfinite(mn) and mn <= -1.0:
                    log1p_shift[col] = (-mn) + 1.0
                else:
                    log1p_shift[col] = 0.0

        # ROBUST_Z: compute medians + q1/q3 once
        bounds = self.compute_quantile_bounds(df, robust_cols, 0.25, 0.75)
        for col in robust_cols:
            q1, q3 = bounds[col]
            iqr = float(q3 - q1)
            if (not np.isfinite(iqr)) or iqr == 0.0:
                iqr = 1.0
            robust_iqr[col] = iqr

        # MINMAX: compute mins/maxs once
        if minmax_cols:
            mins = df[minmax_cols].min().compute()
            maxs = df[minmax_cols].max().compute()
            for col in minmax_cols:
                mn = float(mins[col])
                mx = float(maxs[col])
                if (not np.isfinite(mx - mn)) or (mx - mn) == 0.0:
                    # still store; normalize() will guard
                    pass
                minmax_min[col] = mn
                minmax_max[col] = mx

        normalizer = NormalizationArtifacts(
            log1p_shift=log1p_shift,
            robust_median=robust_median,
            robust_iqr=robust_iqr,
            minmax_min=minmax_min,
            minmax_max=minmax_max,
        )
        return normalizer

    def normalize(
        self,
        normalizer: NormalizationArtifacts,
        df: dd.DataFrame,
        cols: List[ColumnLike],
    ) -> dd.DataFrame:
        target_cols = self.select(df, cols)

        log_cols = [c for c in target_cols if c in normalizer.log1p_shift]
        robust_cols = [c for c in target_cols if c in normalizer.robust_median]
        minmax_cols = [c for c in target_cols if c in normalizer.minmax_min]

        self.ensure_numeric(df, list(set(log_cols + robust_cols + minmax_cols)))

        # LOG1P
        for col in log_cols:
            shift = normalizer.log1p_shift.get(col, 0.0)
            x = df[col] + shift if shift else df[col]
            df[col] = np.log1p(x)

        # ROBUST_Z
        for col in robust_cols:
            med = normalizer.robust_median[col]
            iqr = normalizer.robust_iqr.get(col, 1.0) or 1.0
            if (not np.isfinite(iqr)) or iqr == 0.0:
                iqr = 1.0
            df[col] = (df[col] - med) / iqr

        # MINMAX
        for col in minmax_cols:
            mn = normalizer.minmax_min[col]
            mx = normalizer.minmax_max[col]
            denom = mx - mn
            if (not np.isfinite(denom)) or denom == 0.0:
                denom = 1.0
            df[col] = (df[col] - mn) / denom

        return df

    #########################################################################

    def fit_imputer(
        self,
        df: dd.DataFrame,
        cols: List[ColumnLike],
    ) -> ImputationArtifacts:
        target_cols = self.select(df, cols)

        # classify
        median_cols: List[str] = []
        zero_cols: List[str] = []
        add_indicator_cols: List[str] = []

        for col in target_cols:
            strat = self.get_strategy(col)
            if strat is None:
                continue

            if strat.imputer in (
                ImputerPolicy.MEDIAN,
                ImputerPolicy.MEDIAN_WITH_MISSING_INDICATOR,
            ):
                median_cols.append(col)
            if strat.imputer in (
                ImputerPolicy.ZERO,
                ImputerPolicy.ZERO_WITH_MISSING_INDICATOR,
            ):
                zero_cols.append(col)
            if strat.imputer in (
                ImputerPolicy.MEDIAN_WITH_MISSING_INDICATOR,
                ImputerPolicy.ZERO_WITH_MISSING_INDICATOR,
            ):
                add_indicator_cols.append(col)

        # ensure numeric where needed
        self.ensure_numeric(df, list(set(median_cols + zero_cols)))

        medians: Dict[str, float] = {}
        if median_cols:
            # compute in one batch
            m = df[median_cols].median().compute()
            medians = {c: float(m[c]) for c in median_cols}

        return ImputationArtifacts(
            medians=medians,
            add_missing_indicator=sorted(set(add_indicator_cols)),
            zero_fill=sorted(set(zero_cols)),
        )

    def impute(
        self,
        artifacts: ImputationArtifacts,
        df: dd.DataFrame,
        cols: List[ColumnLike],
    ) -> dd.DataFrame:
        target_cols = self.select(df, cols)

        # missing indicators first
        for col in artifacts.add_missing_indicator:
            if col in df.columns and col in target_cols:
                self.__add_missing_indicator(df, col)

        # numeric coercion for columns we will fill
        fill_cols = set(artifacts.medians.keys()) | set(artifacts.zero_fill)
        self.ensure_numeric(
            df, [c for c in fill_cols if c in df.columns and c in target_cols]
        )

        # median fills
        for col, med in artifacts.medians.items():
            if col in df.columns and col in target_cols:
                df[col] = df[col].fillna(med)

        # zero fills
        for col in artifacts.zero_fill:
            if col in df.columns and col in target_cols:
                df[col] = df[col].fillna(0)

        return df

    def __add_missing_indicator(self, df: dd.DataFrame, col: str) -> None:
        ind = f"{col}__is_missing"
        if ind not in df.columns:
            df[ind] = df[col].isna().astype("int8")

    ##############################################################################

    def fit_outliers(
        self,
        df: dd.DataFrame,
        cols: List[ColumnLike],
    ) -> OutlierArtifacts:
        target_cols = self.select(df, cols)

        # bucket columns by outlier policy
        pctl_cols: Dict[Tuple[float, float], List[str]] = {}  # group by (low_q, high_q)
        iqr_cols: Dict[float, List[str]] = {}  # group by iqr_k
        clip_then_pctl: Dict[Tuple[float, float], List[str]] = {}

        hard_clip: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

        for col in target_cols:
            strat = self.get_strategy(col)
            if strat is None or strat.outlier == OutlierPolicy.NONE:
                continue

            # capture hard bounds (even if None)
            hard_clip[col] = (strat.hard_min, strat.hard_max)

            if strat.outlier in (OutlierPolicy.PCTL_CLIP, OutlierPolicy.PCTL_FILTER):
                pctl_cols.setdefault((strat.pctl_low, strat.pctl_high), []).append(col)
            elif strat.outlier == OutlierPolicy.CLIP_THEN_PCTL_CLIP:
                clip_then_pctl.setdefault((strat.pctl_low, strat.pctl_high), []).append(
                    col
                )
            elif strat.outlier == OutlierPolicy.IQR_FILTER:
                iqr_cols.setdefault(strat.iqr_k, []).append(col)

        # ensure numeric
        cols_numeric = []
        for g in (
            list(pctl_cols.values())
            + list(clip_then_pctl.values())
            + list(iqr_cols.values())
        ):
            cols_numeric.extend(g)
        self.ensure_numeric(df, list(set(cols_numeric)))

        pctl_bounds: Dict[str, Tuple[float, float]] = {}
        iqr_bounds: Dict[str, Tuple[float, float]] = {}

        # percentile bounds (batch compute per (low_q, high_q))
        # percentile bounds
        for (lq, hq), group in pctl_cols.items():
            group = self.existing(df, group)
            if not group:
                continue
            pctl_bounds.update(self.compute_quantile_bounds(df, group, lq, hq))

        # clip-then-percentile bounds
        for (lq, hq), group in clip_then_pctl.items():
            group = self.existing(df, group)
            if not group:
                continue
            pctl_bounds.update(self.compute_quantile_bounds(df, group, lq, hq))
        # IQR bounds (batch compute q1/q3 for all iqr cols)
        if iqr_cols:
            all_iqr = sorted({c for group in iqr_cols.values() for c in group})
            all_iqr = self.existing(df, all_iqr)
            if all_iqr:
                q = df[all_iqr].quantile([0.25, 0.75]).compute()
                q1 = q.loc[0.25]
                q3 = q.loc[0.75]
                for k, group in iqr_cols.items():
                    for c in group:
                        if c not in all_iqr:
                            continue
                        iqr = float(q3[c] - q1[c])
                        lo = float(q1[c] - k * iqr)
                        hi = float(q3[c] + k * iqr)
                        iqr_bounds[c] = (lo, hi)

        return OutlierArtifacts(
            pctl_bounds=pctl_bounds,
            iqr_bounds=iqr_bounds,
            hard_clip=hard_clip,
        )

    # ============================================================
    # TRANSFORM: apply outlier handling to any df (inference-safe)
    # mode="clip" (winsorize) or mode="drop"
    # ============================================================
    def handle_outliers(
        self,
        artifacts: OutlierArtifacts,
        df: dd.DataFrame,
        cols: List[ColumnLike],
        *,
        mode: str = "clip",
    ) -> dd.DataFrame:
        if mode not in {"clip", "drop"}:
            raise ValueError("mode must be 'clip' or 'drop'")

        target_cols = self.select(df, cols)

        # ensure numeric for columns we touch
        touch = set(artifacts.pctl_bounds) | set(artifacts.iqr_bounds)
        self.ensure_numeric(
            df, [c for c in touch if c in df.columns and c in target_cols]
        )

        global_mask = None

        # 1) hard clip first (for CLIP_THEN_PCTL_CLIP and percent-like features)
        for col, (hmin, hmax) in artifacts.hard_clip.items():
            if col not in df.columns or col not in target_cols:
                continue
            if hmin is not None or hmax is not None:
                df[col] = df[col].clip(lower=hmin, upper=hmax)

        # 2) percentile bounds
        for col, (lo, hi) in artifacts.pctl_bounds.items():
            if col not in df.columns or col not in target_cols:
                continue
            if mode == "clip":
                df[col] = df[col].clip(lower=lo, upper=hi)
            else:
                m = df[col].between(lo, hi) | df[col].isna()
                global_mask = m if global_mask is None else (global_mask & m)

        # 3) IQR bounds (drop is typical; clip is allowed but less common)
        for col, (lo, hi) in artifacts.iqr_bounds.items():
            if col not in df.columns or col not in target_cols:
                continue
            if mode == "clip":
                df[col] = df[col].clip(lower=lo, upper=hi)
            else:
                m = df[col].between(lo, hi) | df[col].isna()
                global_mask = m if global_mask is None else (global_mask & m)

        if mode == "drop" and global_mask is not None:
            df = df[global_mask]

        return df

    def compute_quantile_bounds(
        self,
        df: dd.DataFrame,
        cols: List[str],
        low_q: float,
        high_q: float,
    ) -> Dict[str, Tuple[float, float]]:
        """
        Returns {col: (low, high)} for each col.
        Handles both DataFrame and Series outputs from quantile().compute().
        """
        cols = self.existing(df, cols)
        if not cols:
            return {}

        q = df[cols].quantile([low_q, high_q]).compute()

        bounds: Dict[str, Tuple[float, float]] = {}

        if isinstance(q, pd.DataFrame):
            # index = quantiles, columns = cols
            for c in cols:
                bounds[c] = (float(q.loc[low_q, c]), float(q.loc[high_q, c]))
            return bounds

        if isinstance(q, pd.Series):
            # Often a MultiIndex: (quantile, column)
            if isinstance(q.index, pd.MultiIndex):
                for c in cols:
                    bounds[c] = (float(q.loc[(low_q, c)]), float(q.loc[(high_q, c)]))
                return bounds

            # Single column case where index is just quantiles
            if len(cols) == 1 and low_q in q.index and high_q in q.index:
                c = cols[0]
                bounds[c] = (float(q.loc[low_q]), float(q.loc[high_q]))
                return bounds

        raise TypeError(
            f"Unexpected quantile result type: {type(q)}; index={getattr(q, 'index', None)}"
        )
