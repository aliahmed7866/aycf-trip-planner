import os
import glob
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

import pandas as pd
from dateutil import parser as dtparser

# The dataset in the repo is already parsed daily into CSV runs.
# Typical columns: departure_from, departure_to, availability_start, availability_end, data_generated
REQUIRED_COLS = {"departure_from", "departure_to"}

DEFAULT_BASES = ["Liverpool", "London Luton", "Birmingham", "Leeds/Bradford"]
DEFAULT_HUBS = ["Bucharest", "Budapest", "Warsaw", "Gdansk", "Krakow", "Katowice", "Liverpool", "London Luton"]
DEFAULT_TARGETS = [
    "Kutaisi", "Yerevan", "Amman",
    "Dubai", "Abu Dhabi",
    "Hurghada", "Sharm el-Sheikh",
    "Tel Aviv", "Marrakech",
]

# UI-friendly labels → dataset city names
# The dataset typically uses "London" for London-area departures; we expose "London Luton" in the UI.
CITY_ALIASES = {
    "London Luton": "London",
    "London (Luton)": "London",
}

def normalise_city(name: str) -> str:
    name = (name or "").strip()
    return CITY_ALIASES.get(name, name)

def _safe_parse_dt(s: Optional[str]):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return pd.NaT
    try:
        return pd.Timestamp(dtparser.parse(str(s)))
    except Exception:
        return pd.NaT

@dataclass
class Suggestion:
    base: str
    hub: str
    target: str
    return_hub: str
    base_to_hub_freq: int
    hub_to_target_freq: int
    target_to_return_hub_freq: int
    return_hub_to_base_freq: int
    score: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "itinerary": f"{self.base} → {self.hub} → {self.target}",
            "return": f"{self.target} → {self.return_hub} → {self.base}",
            "base_to_hub": self.base_to_hub_freq,
            "hub_to_target": self.hub_to_target_freq,
            "target_to_hub": self.target_to_return_hub_freq,
            "hub_to_base": self.return_hub_to_base_freq,
            "score": round(self.score, 2),
        }

class AYCFPlanner:
    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.file_count = 0
        self.last_run_count = 0

    def _load_runs(self) -> pd.DataFrame:
        paths = sorted(glob.glob(os.path.join(self.data_dir, "**", "*.csv"), recursive=True))
        if not paths:
            raise FileNotFoundError(
                f"No CSV runs found in {self.data_dir}. "
                f"Set AYCF_DATA_DIR to the repo's data folder (e.g. .../wizzair-aycf-availability-main/data)."
            )

        frames = []
        for p in paths:
            try:
                df = pd.read_csv(p)
                if not REQUIRED_COLS.issubset(df.columns):
                    continue
                df = df.copy()
                df["source_file"] = os.path.basename(p)
                # Normalise common time column
                if "data_generated" in df.columns:
                    df["run_ts"] = df["data_generated"].apply(_safe_parse_dt)
                elif "run_ts" in df.columns:
                    df["run_ts"] = df["run_ts"].apply(_safe_parse_dt)
                else:
                    df["run_ts"] = pd.NaT
                frames.append(df)
            except Exception:
                continue

        if not frames:
            raise ValueError("Found CSV files but none with expected columns (departure_from, departure_to).")

        out = pd.concat(frames, ignore_index=True)
        self.file_count = len(paths)
        self.last_run_count = int(out["source_file"].nunique())
        out["departure_from"] = out["departure_from"].astype(str).str.strip().apply(normalise_city)
        out["departure_to"] = out["departure_to"].astype(str).str.strip().apply(normalise_city)
        return out

    def _filter_by_date(self, df: pd.DataFrame, start_date: Optional[str], end_date: Optional[str]) -> pd.DataFrame:
        # Kept for compatibility; in v3 we mainly use lookback-days for stability.

        # Start/end are user-provided dates (YYYY-MM-DD). We filter on run_ts (data_generated) when present.
        if "run_ts" not in df.columns:
            return df

        if start_date:
            start = pd.Timestamp(dtparser.parse(start_date)).tz_localize(None)
        else:
            # default: last 180 days
            start = pd.Timestamp(datetime.now() - pd.Timedelta(days=180)).tz_localize(None)

        if end_date:
            end = pd.Timestamp(dtparser.parse(end_date)).tz_localize(None) + pd.Timedelta(days=1)  # inclusive end date
        else:
            end = pd.Timestamp(datetime.now()).tz_localize(None) + pd.Timedelta(days=1)

        # If run_ts is mostly NaT, we won't drop them; treat NaT as "unknown run time" and keep
        mask = df["run_ts"].isna() | ((df["run_ts"] >= start) & (df["run_ts"] < end))
        return df[mask].copy()


    def _filter_by_lookback(self, df: pd.DataFrame, lookback_days: int) -> pd.DataFrame:
        if "run_ts" not in df.columns:
            return df
        cutoff = pd.Timestamp(datetime.now() - pd.Timedelta(days=int(lookback_days))).tz_localize(None)
        mask = df["run_ts"].isna() | (df["run_ts"] >= cutoff)
        return df[mask].copy()

    def route_counts(self, lookback_days: int) -> pd.DataFrame:
        df = self._load_runs()
        df = self._filter_by_lookback(df, lookback_days)
        counts = df.groupby(["departure_from", "departure_to"]).size().reset_index(name="appearances")
        return counts.sort_values("appearances", ascending=False)

    def suggest_itineraries(
        self,
        lookback_days: int,
        min_transfer_minutes: int,
        start_date: Optional[str],
        end_date: Optional[str],
        bases: List[str],
        hubs: List[str],
        targets: List[str],
        require_return_to_base: bool,
        top_n: int = 25
    ) -> List[Dict[str, Any]]:
        counts = self.route_counts(lookback_days)

        bases_set = set([normalise_city(b) for b in bases if str(b).strip()])
        hubs_set = set([normalise_city(h) for h in hubs if str(h).strip()])
        targets_set = set([normalise_city(t) for t in targets if str(t).strip()])

        # base -> hub
        bh = counts[counts["departure_from"].isin(bases_set) & counts["departure_to"].isin(hubs_set)].copy()
        bh = bh.rename(columns={
            "departure_from": "base",
            "departure_to": "hub",
            "appearances": "base_to_hub"
        })

        # hub -> target
        ht = counts[counts["departure_from"].isin(hubs_set) & counts["departure_to"].isin(targets_set)].copy()
        ht = ht.rename(columns={
            "departure_from": "hub",
            "departure_to": "target",
            "appearances": "hub_to_target"
        })

        merged = bh.merge(ht, on="hub", how="inner")

        if merged.empty:
            return []

        # target -> return hub (any hub)
        th = counts[counts["departure_from"].isin(targets_set) & counts["departure_to"].isin(hubs_set)].copy()
        th = th.rename(columns={
            "departure_from": "target",
            "departure_to": "return_hub",
            "appearances": "target_to_hub"
        })

        merged = merged.merge(th, on="target", how="left")
        merged["target_to_hub"] = merged["target_to_hub"].fillna(0).astype(int)
        merged["return_hub"] = merged["return_hub"].fillna(merged["hub"])

        # return hub -> base
        hb = counts[counts["departure_from"].isin(hubs_set) & counts["departure_to"].isin(bases_set)].copy()
        hb = hb.rename(columns={
            "departure_from": "return_hub",
            "departure_to": "base",
            "appearances": "hub_to_base"
        })
        merged = merged.merge(hb, on=["return_hub", "base"], how="left")
        merged["hub_to_base"] = merged["hub_to_base"].fillna(0).astype(int)

        if require_return_to_base:
            merged = merged[merged["hub_to_base"] > 0]

        # score:
        # - favour stable base->hub and hub->target
        # - favour having multiple ways back (target->hub)
        # - if requiring return to base, the hub->base is included in score too
        merged["score"] = (
            merged["base_to_hub"].astype(float)
            + merged["hub_to_target"].astype(float)
            + 1.2 * merged["target_to_hub"].astype(float)
            + (0.8 * merged["hub_to_base"].astype(float) if require_return_to_base else 0.3 * merged["hub_to_base"].astype(float))
        )

        merged = merged.sort_values("score", ascending=False).head(top_n)

        suggestions = []
        for _, r in merged.iterrows():
            s = Suggestion(
                base=str(r["base"]),
                hub=str(r["hub"]),
                target=str(r["target"]),
                return_hub=str(r["return_hub"]),
                base_to_hub_freq=int(r["base_to_hub"]),
                hub_to_target_freq=int(r["hub_to_target"]),
                target_to_return_hub_freq=int(r["target_to_hub"]),
                return_hub_to_base_freq=int(r["hub_to_base"]),
                score=float(r["score"]),
            )
            suggestions.append(s.to_dict())
        return suggestions

    def ui_defaults(self) -> Dict[str, Any]:
        # Provide lists for the UI; you can expand these over time
        return {
            "base_options": DEFAULT_BASES,
            "hub_options": DEFAULT_HUBS,
            "target_options": DEFAULT_TARGETS,
            "default_bases": ["Liverpool", "London Luton"],
            "default_hubs": ["Bucharest", "Budapest", "Warsaw", "Gdansk", "Krakow", "Katowice", "Liverpool", "London Luton"],
            "default_targets": ["Kutaisi", "Yerevan", "Amman", "Dubai", "Abu Dhabi", "Hurghada", "Sharm el-Sheikh"],
        }
