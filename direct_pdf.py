import os
import re
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
import pdfplumber
import requests

DEFAULT_PDF_URL = "https://multipass.wizzair.com/aycf-availability.pdf"
_TS = r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}"


def _clean(value) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\n", " ")).strip()


def _valid_route_pair(a: str, b: str) -> bool:
    if not a or not b:
        return False
    low_a, low_b = a.lower(), b.lower()
    if low_a.startswith("departure") or low_b.startswith("arrival"):
        return False
    blocked = ("please note", "departure period", "last run", "page ", "terms & conditions")
    return not any(token in low_a or token in low_b for token in blocked)


def download_pdf(cache_root: str, url: str = DEFAULT_PDF_URL) -> Path:
    target_dir = Path(cache_root) / "direct-pdf"
    target_dir.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=45, headers={"User-Agent": "AYCF-Trip-Planner/1.0"})
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise RuntimeError("Wizz AYCF URL did not return a PDF.")
    temp = target_dir / "latest.pdf.tmp"
    target = target_dir / "latest.pdf"
    temp.write_bytes(response.content)
    temp.replace(target)
    return target


def parse_pdf(path: Path) -> Tuple[pd.DataFrame, datetime, datetime, datetime]:
    route_rows = []
    all_text = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            all_text.append(page.extract_text() or "")
            for table in page.extract_tables() or []:
                for row in table or []:
                    if not row or len(row) < 2:
                        continue
                    # Depending on PDF extraction, a page may be represented as
                    # one 4-column table or multiple 2-column tables. Treat each
                    # adjacent column pair as departure/arrival.
                    for index in range(0, len(row) - 1, 2):
                        a, b = _clean(row[index]), _clean(row[index + 1])
                        if _valid_route_pair(a, b):
                            route_rows.append((a, b))

    text = "\n".join(all_text)
    run_match = re.search(rf"Last\s+run:\s*({_TS})\s*\((?:CET|CEST)\)", text, re.I)
    period_match = re.search(rf"Departure\s+period:\s*({_TS})\s*-\s*({_TS})\s*\((?:CET|CEST)\)", text, re.I)
    if not run_match or not period_match:
        raise RuntimeError("Could not read publication metadata from Wizz AYCF PDF.")
    if not route_rows:
        raise RuntimeError("Could not extract route table from Wizz AYCF PDF.")

    generated = datetime.fromisoformat(run_match.group(1))
    start = datetime.fromisoformat(period_match.group(1))
    end = datetime.fromisoformat(period_match.group(2))
    df = pd.DataFrame(sorted(set(route_rows)), columns=["departure_from", "departure_to"])
    df["availability_start"] = start.isoformat()
    df["availability_end"] = end.isoformat()
    df["data_generated"] = generated.isoformat()
    return df, generated, start, end


def refresh_direct_snapshot(cache_root: str, url: str = DEFAULT_PDF_URL) -> Tuple[str, pd.DataFrame, datetime, datetime, datetime]:
    pdf_path = download_pdf(cache_root, url)
    df, generated, start, end = parse_pdf(pdf_path)
    data_dir = Path(cache_root) / "direct-data"
    data_dir.mkdir(parents=True, exist_ok=True)
    run_id = generated.isoformat().replace(":", "_")
    target = data_dir / f"{run_id}.csv"
    if not target.exists():
        df.to_csv(target, index=False)
    return str(data_dir), df, generated, start, end
