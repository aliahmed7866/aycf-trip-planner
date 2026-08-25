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
            text = page.extract_text() or ""
            all_text.append(text)
            tables = page.extract_tables() or []
            for table in tables:
                for row in table or []:
                    if not row or len(row) < 2:
                        continue
                    a, b = _clean(row[0]), _clean(row[1])
                    if not a or not b:
                        continue
                    if a.lower().startswith("departure") or b.lower().startswith("arrival"):
                        continue
                    if a.lower().startswith("please note") or "departure period" in a.lower():
                        continue
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
