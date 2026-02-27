import pandas as pd

REQUIRED_COLS = [
    "First Name", "Last Name", "Email", "Country", "Application Date",
    "YOE", "Seniority", "Technology", "Code Challenge Score", "Technical Interview Score"
]

def extract(csv_path: str) -> pd.DataFrame:
    # OJO: el CSV viene separado por ; (punto y coma)
    df = pd.read_csv(csv_path, sep=";")

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en el CSV: {missing}")

    return df