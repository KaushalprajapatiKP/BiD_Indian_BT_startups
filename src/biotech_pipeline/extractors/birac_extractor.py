"""
BIRAC Data Extractor – handles PDF, Excel, and URL sources.
"""

import logging
import pandas as pd
import pdfplumber
from tabula import read_pdf
from io import BytesIO
import requests
from typing import List, Dict

from biotech_pipeline.utils.helpers import clean_text
from biotech_pipeline.utils.exceptions import ExtractionError

logger = logging.getLogger(__name__)

class BiracExtractor:
    def __init__(self, config: Dict):
        self.config = config

    def extract_from_pdf(self, file_path: str) -> List[Dict]:
        try:
            records = []
            # Text extraction
            with pdfplumber.open(file_path) as pdf:
                text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                records += self._parse_text(text)
            # Tabular extraction
            dfs = read_pdf(file_path, pages="all", multiple_tables=True)
            for df in dfs:
                records += self._parse_dataframe(df)
            return records
        except Exception as e:
            raise ExtractionError(f"PDF extraction failed ({file_path}): {e}")

    def extract_from_excel(self, file_path: str) -> List[Dict]:
        try:
            df = pd.read_excel(file_path, sheet_name="Sheet2")
            return self._parse_dataframe(df)
        except Exception as e:
            raise ExtractionError(f"Excel extraction failed ({file_path}): {e}")

    def extract_from_url(self, url: str) -> List[Dict]:
        try:
            resp = requests.get(url, timeout=self.config["scraping"]["timeout"])
            resp.raise_for_status()
            content = resp.content
            if url.lower().endswith(".pdf"):
                return self.extract_from_pdf(BytesIO(content))
            else:
                df = pd.read_excel(BytesIO(content))
                return self._parse_dataframe(df)
        except Exception as e:
            raise ExtractionError(f"URL extraction failed ({url}): {e}")

    def _parse_text(self, text: str) -> List[Dict]:
        records = []
        for line in text.splitlines():
            line = clean_text(line)
            if line.upper().startswith("BIRAC/"):
                parts = line.split()
                records.append({"big_award_id": parts[0]})
        return records

    def _parse_dataframe(self, df: pd.DataFrame) -> List[Dict]:
        records = []
        df.columns = [clean_text(str(c)).lower() for c in df.columns]
        for _, row in df.iterrows():
            rec = {}
            if "award_id" in row:
                rec["big_award_id"] = clean_text(str(row["award_id"]))
            if "company_name" in row:
                rec["registered_name"] = clean_text(str(row["company_name"]))
            if "year" in row:
                try:
                    rec["big_award_year"] = int(row["year"])
                except:
                    pass
            if rec.get("big_award_id") and rec.get("registered_name"):
                records.append(rec)
        return records
