import pandas as pd

def transform(raw: pd.DataFrame):
    df = raw.copy()

    # 1) Convertir tipos
    df["Application Date"] = pd.to_datetime(df["Application Date"], errors="coerce")
    df["YOE"] = pd.to_numeric(df["YOE"], errors="coerce")
    df["Code Challenge Score"] = pd.to_numeric(df["Code Challenge Score"], errors="coerce")
    df["Technical Interview Score"] = pd.to_numeric(df["Technical Interview Score"], errors="coerce")

    # 2) Quitar filas malas (nulos)
    df = df.dropna(subset=[
        "Application Date", "YOE", "Code Challenge Score", "Technical Interview Score",
        "First Name", "Last Name", "Email", "Country", "Seniority", "Technology"
    ])

    # 3) Regla HIRED: Code >= 7 y Interview >= 7
    df["is_hired"] = ((df["Code Challenge Score"] >= 7) & (df["Technical Interview Score"] >= 7))

    # 4) Dimensiones (sin surrogate aquí; Postgres lo genera con SERIAL)
    dim_candidate = df[["First Name", "Last Name", "Email"]].drop_duplicates().reset_index(drop=True)

    dim_country = (df[["Country"]].drop_duplicates().reset_index(drop=True)
                   .rename(columns={"Country": "country"}))

    dim_seniority = (df[["Seniority"]].drop_duplicates().reset_index(drop=True)
                     .rename(columns={"Seniority": "seniority"}))

    dim_technology = (df[["Technology"]].drop_duplicates().reset_index(drop=True)
                      .rename(columns={"Technology": "technology"}))

    dim_date = df[["Application Date"]].drop_duplicates().reset_index(drop=True)
    dim_date["application_date"] = dim_date["Application Date"].dt.date
    dim_date["year"] = dim_date["Application Date"].dt.year
    dim_date["month"] = dim_date["Application Date"].dt.month
    dim_date["day"] = dim_date["Application Date"].dt.day
    dim_date = dim_date[["application_date", "year", "month", "day"]]

    # 5) Fact “cruda” (aún sin keys, luego las mapeamos desde la BD)
    fact_raw = df[[
        "First Name", "Last Name", "Email",
        "Country", "Seniority", "Technology",
        "Application Date", "YOE",
        "Code Challenge Score", "Technical Interview Score",
        "is_hired"
    ]].copy()

    fact_raw["application_date"] = fact_raw["Application Date"].dt.date

    fact_raw = fact_raw.rename(columns={
        "YOE": "yoe",
        "Code Challenge Score": "code_challenge_score",
        "Technical Interview Score": "technical_interview_score",
        "Country": "country",
        "Seniority": "seniority",
        "Technology": "technology",
    })

    return dim_candidate, dim_country, dim_date, dim_seniority, dim_technology, fact_raw