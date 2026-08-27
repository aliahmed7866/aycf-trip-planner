import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests

try:
    import pdfplumber
except Exception:
    pdfplumber = None

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


def _timestamps_after_label(text: str, label_pattern: str, count: int):
    label = re.search(label_pattern, text, re.I)
    if not label:
        return []
    # Metadata is at the beginning/end of the first page, so a bounded window
    # avoids accidentally consuming unrelated timestamps elsewhere in the PDF.
    window = text[label.end(): label.end() + 500]
    return re.findall(_TS, window, re.I)[:count]


def _parse_metadata(text: str):
    # Wizz has varied the PDF extraction layout over time: labels/timestamps may
    # be on the same or following line, the timezone wording may change, and the
    # range separator may be '-' or a Unicode dash. Anchor to the labels and
    # parse timestamp values independently of that presentation formatting.
    run_values = _timestamps_after_label(text, r"Last\s+run\s*: ?", 1)
    period_values = _timestamps_after_label(text, r"Departure\s+period\s*: ?", 2)
    if len(run_values) != 1 or len(period_values) != 2:
        raise RuntimeError("Could not read publication metadata from Wizz AYCF PDF.")
    return (
        datetime.fromisoformat(run_values[0]),
        datetime.fromisoformat(period_values[0]),
        datetime.fromisoformat(period_values[1]),
    )


def _parse_with_pdftotext(path: Path):
    exe = shutil.which("pdftotext")
    if not exe:
        raise RuntimeError("PDF parsing needs pdfplumber or the pdftotext command (Termux: pkg install poppler).")
    proc = subprocess.run([exe, "-layout", str(path), "-"], capture_output=True, text=True, timeout=60, check=True)
    text = proc.stdout
    route_rows = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            continue
        cells = [_clean(x) for x in re.split(r"\s{2,}", line.strip()) if _clean(x)]
        for i in range(0, len(cells) - 1, 2):
            a, b = cells[i], cells[i + 1]
            if _valid_route_pair(a, b):
                route_rows.append((a, b))
    if not route_rows:
        raise RuntimeError("Could not extract route table from Wizz AYCF PDF using pdftotext.")
    return text, route_rows


def parse_pdf(path: Path) -> Tuple[pd.DataFrame, datetime, datetime, datetime]:
    route_rows = []
    all_text = []
    if pdfplumber is not None:
        try:
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    all_text.append(page.extract_text() or "")
                    for table in page.extract_tables() or []:
                        for row in table or []:
                            if not row or len(row) < 2:
                                continue
                            for index in range(0, len(row) - 1, 2):
                                a, b = _clean(row[index]), _clean(row[index + 1])
                                if _valid_route_pair(a, b):
                                    route_rows.append((a, b))
        except Exception:
            route_rows = []
            all_text = []

    if not route_rows:
        text, route_rows = _parse_with_pdftotext(path)
    else:
        text = "\n".join(all_text)

    generated, start, end = _parse_metadata(text)
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
