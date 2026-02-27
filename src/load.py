import psycopg2
from psycopg2.extras import execute_values

#Recomendación: más adelante guardamos esto en un .env, pero hoy lo dejamos simple
DB_CONFIG = {
    "host": "localhost",
    "database": "etl_dw",
    "user": "postgres",
    "password": "Papofutbol",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_to_dw(dim_candidate, dim_country, dim_date, dim_seniority, dim_technology, fact_raw):
    conn = get_connection()
    cur = conn.cursor()

    # ---------- INSERT DIMS (con ON CONFLICT para no duplicar) ----------
    execute_values(
        cur,
        """
        INSERT INTO dim_country (country)
        VALUES %s
        ON CONFLICT (country) DO NOTHING
        """,
        dim_country[["country"]].values
    )

    execute_values(
        cur,
        """
        INSERT INTO dim_seniority (seniority)
        VALUES %s
        ON CONFLICT (seniority) DO NOTHING
        """,
        dim_seniority[["seniority"]].values
    )

    execute_values(
        cur,
        """
        INSERT INTO dim_technology (technology)
        VALUES %s
        ON CONFLICT (technology) DO NOTHING
        """,
        dim_technology[["technology"]].values
    )

    # dim_date: application_date es UNIQUE
    execute_values(
        cur,
        """
        INSERT INTO dim_date (application_date, year, month, day)
        VALUES %s
        ON CONFLICT (application_date) DO NOTHING
        """,
        dim_date[["application_date", "year", "month", "day"]].values
    )

    # dim_candidate: no tiene UNIQUE, entonces insertamos “tal cual”
    # (para hacerlo perfecto, podríamos crear UNIQUE(email), pero hoy lo dejamos así)
    execute_values(
        cur,
        """
        INSERT INTO dim_candidate (first_name, last_name, email)
        VALUES %s
        """,
        dim_candidate[["First Name", "Last Name", "Email"]].values
    )

    conn.commit()

    # ---------- CARGAR MAPAS DE KEYS (para armar la FACT) ----------
    cur.execute("SELECT candidate_key, first_name, last_name, email FROM dim_candidate;")
    candidate_map = {(r[1], r[2], r[3]): r[0] for r in cur.fetchall()}

    cur.execute("SELECT country_key, country FROM dim_country;")
    country_map = {r[1]: r[0] for r in cur.fetchall()}

    cur.execute("SELECT seniority_key, seniority FROM dim_seniority;")
    seniority_map = {r[1]: r[0] for r in cur.fetchall()}

    cur.execute("SELECT technology_key, technology FROM dim_technology;")
    technology_map = {r[1]: r[0] for r in cur.fetchall()}

    cur.execute("SELECT date_key, application_date FROM dim_date;")
    date_map = {r[1]: r[0] for r in cur.fetchall()}

    # ---------- ARMAR FACT PARA INSERT ----------
    fact_rows = []
    for _, row in fact_raw.iterrows():
        cand_key = candidate_map.get((row["First Name"], row["Last Name"], row["Email"]))
        ctry_key = country_map.get(row["country"])
        sen_key = seniority_map.get(row["seniority"])
        tech_key = technology_map.get(row["technology"])
        dt_key = date_map.get(row["application_date"])

        if None in [cand_key, ctry_key, sen_key, tech_key, dt_key]:
            # Si algo falla, no insertamos esa fila (seguridad)
            continue

        fact_rows.append((
            cand_key,
            ctry_key,
            dt_key,
            sen_key,
            tech_key,
            int(row["yoe"]),
            int(row["code_challenge_score"]),
            int(row["technical_interview_score"]),
            bool(row["is_hired"])
        ))

    # ---------- INSERT FACT ----------
    execute_values(
        cur,
        """
        INSERT INTO fact_application
        (candidate_key, country_key, date_key, seniority_key, technology_key,
         yoe, code_challenge_score, technical_interview_score, is_hired)
        VALUES %s
        """,
        fact_rows
    )

    conn.commit()
    cur.close()
    conn.close()