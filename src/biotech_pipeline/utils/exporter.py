# src/biotech_pipeline/utils/exporter.py

import pandas as pd
from sqlalchemy.orm import Session
from src.biotech_pipeline.core.database import SessionLocal
from src.biotech_pipeline.utils.logger import get_database_logger
from typing import Optional, Dict

logger = get_database_logger()

from typing import List

def make_datetimes_timezone_naive(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include=['datetimetz']).columns:
        df[col] = df[col].dt.tz_localize(None)
    return df


def export_tables_to_excel(
    excel_path: str,
    table_names: List[str]
) -> None:
    session: Session = SessionLocal()
    try:
        with session.bind.connect() as conn:
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                sheet_written = False
                for table_name in table_names:
                    if table_name.isidentifier():
                        sql = f"SELECT * FROM {table_name}"
                    else:
                        sql = table_name

                    logger.info("Exporting sheet '%s' with SQL: %s", table_name, sql)
                    df = pd.read_sql(sql, conn)

                    # Convert timezone-aware datetime columns to naive (remove tz)
                    df = make_datetimes_timezone_naive(df)
                    
                    if df.empty:
                        logger.warning("No data for table '%s', skipping sheet", table_name)
                        continue

                    df.to_excel(writer, sheet_name=table_name, index=False)
                    sheet_written = True

                if not sheet_written:
                    raise ValueError("No data to export: Excel must have at least one visible sheet.")

        logger.info("Exported Excel file successfully: %s", excel_path)

    except Exception:
        logger.error("Failed to export Excel file %s", excel_path, exc_info=True)
        raise
    finally:
        session.close()
