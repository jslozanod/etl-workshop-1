import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

# =========================
# Config DB (PostgreSQL)
# =========================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "database": os.getenv("PGDATABASE", "etl_dw"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
    "port": int(os.getenv("PGPORT", "5432")),
}

OUTPUT_DIR = "visualizations"
PROCESSED_DIR = os.path.join("data", "processed")

# =========================
# Premium theme (dark + neon)
# Profe esta parte la hice con IA para que las garficas se vieran bonitas
# =========================
BG = "#0B0F14"          # background
PANEL = "#0F1621"       # axes background
GRID = "#2A3441"        # grid lines
TEXT = "#E8EEF6"        # text
MUTED = "#A6B3C2"       # secondary text

NEON_CYAN = "#00C2FF"
NEON_GREEN = "#00FFAA"
NEON_PURPLE = "#A78BFA"
NEON_PINK = "#FF4DCC"
NEON_YELLOW = "#FFD166"

COUNTRY_COLORS = {
    "United States": NEON_CYAN,
    "Brazil": NEON_GREEN,
    "Colombia": NEON_PURPLE,
    "Ecuador": NEON_PINK
}

plt.rcParams.update({
    "figure.facecolor": BG,
    "axes.facecolor": PANEL,
    "axes.edgecolor": GRID,
    "axes.labelcolor": TEXT,
    "xtick.color": TEXT,
    "ytick.color": TEXT,
    "text.color": TEXT,
    "font.size": 11,
    "axes.titleweight": "bold",
})

# =========================
# DB helpers
# =========================
def get_connection():
    if not DB_CONFIG["password"]:
        raise ValueError("PGPASSWORD no está definida.")

    return psycopg2.connect(**DB_CONFIG)


def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


# =========================
# IO helpers
# =========================
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def save_fig(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    plt.tight_layout()
    plt.savefig(path, dpi=200, facecolor=BG)
    plt.close()
    print(f"Saved: {path}")


def save_csv(df: pd.DataFrame, filename: str):
    path = os.path.join(PROCESSED_DIR, filename)
    df.to_csv(path, index=False)
    print(f"Saved: {path}")


# =========================
# Styling helpers
# =========================
def style_axes(ax, title: str, subtitle: str | None = None):
    ax.set_title(title, pad=14)
    if subtitle:
        ax.text(0, 1.02, subtitle, transform=ax.transAxes, color=MUTED, fontsize=10, va="bottom")
    ax.grid(True, axis="both", alpha=0.35, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for spine in ax.spines.values():
        spine.set_color(GRID)


def add_bar_labels_horizontal(ax, fmt="{:,.0f}", color=TEXT):
    for p in ax.patches:
        w = p.get_width()
        ax.text(
            w,
            p.get_y() + p.get_height() / 2,
            "  " + fmt.format(w),
            va="center",
            color=color,
            path_effects=[pe.withStroke(linewidth=3, foreground=BG)]
        )


def bar_shadow(bars):
    for b in bars:
        b.set_path_effects([
            pe.SimplePatchShadow(offset=(2, -2), alpha=0.35),
            pe.Normal()
        ])


# =========================
# KPI QUERIES
# =========================
Q1_TECH = """
SELECT t.technology, COUNT(*) AS hires
FROM fact_application f
JOIN dim_technology t ON f.technology_key = t.technology_key
WHERE f.is_hired = TRUE
GROUP BY t.technology
ORDER BY hires DESC;
"""

Q2_YEAR = """
SELECT d.year, COUNT(*) AS hires
FROM fact_application f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_hired = TRUE
GROUP BY d.year
ORDER BY d.year;
"""

Q3_SEN = """
SELECT s.seniority, COUNT(*) AS hires
FROM fact_application f
JOIN dim_seniority s ON f.seniority_key = s.seniority_key
WHERE f.is_hired = TRUE
GROUP BY s.seniority
ORDER BY hires DESC;
"""

Q4_COUNTRY = """
SELECT c.country, d.year, COUNT(*) AS hires
FROM fact_application f
JOIN dim_country c ON f.country_key = c.country_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_hired = TRUE
  AND c.country IN ('United States', 'Brazil', 'Colombia', 'Ecuador')
GROUP BY c.country, d.year
ORDER BY c.country, d.year;
"""

Q5_HIRERATE = """
SELECT
  100.0 * SUM(CASE WHEN is_hired THEN 1 ELSE 0 END) / COUNT(*) AS hire_rate_percent
FROM fact_application;
"""

Q6_AVG = """
SELECT
  AVG(code_challenge_score) AS avg_code_score,
  AVG(technical_interview_score) AS avg_interview_score
FROM fact_application
WHERE is_hired = TRUE;
"""


# =========================
# KPI 1 — Hires by Technology (Top 15)
# =========================
def plot_kpi_1():
    df = run_query(Q1_TECH)
    save_csv(df, "kpi_1_hires_by_technology.csv")

    top = df.head(15).sort_values("hires")

    plt.figure(figsize=(12, 7))
    ax = plt.gca()

    # Gradient-ish neon: scale hires -> color intensity (use colormap)
    norm = top["hires"] / top["hires"].max()
    colors = [(0.0, 0.76*n + 0.24, 1.0) for n in norm]  # cyan-ish varying
    bars = ax.barh(top["technology"], top["hires"], color=colors)

    bar_shadow(bars)
    style_axes(ax, "KPI 1 — Hires by Technology (Top 15)", "Fuente: Data Warehouse (PostgreSQL)")
    ax.set_xlabel("Hires")
    ax.set_ylabel("Technology")

    add_bar_labels_horizontal(ax)

    save_fig("kpi_1_hires_by_technology_top15.png")


# =========================
# KPI 2 — Hires by Year
# =========================
def plot_kpi_2():
    df = run_query(Q2_YEAR)
    save_csv(df, "kpi_2_hires_by_year.csv")

    plt.figure(figsize=(10, 5))
    ax = plt.gca()

    ax.plot(df["year"], df["hires"], marker="o", linewidth=3, markersize=8, color=NEON_CYAN)
    # Glow effect on line markers
    for line in ax.lines:
        line.set_path_effects([pe.Stroke(linewidth=6, foreground="#0B2A3D"), pe.Normal()])

    style_axes(ax, "KPI 2 — Hires by Year", "Contrataciones por año (solo HIRED)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Hires")

    for x, y in zip(df["year"], df["hires"]):
        ax.text(x, y, f" {int(y)}", va="bottom", color=TEXT,
                path_effects=[pe.withStroke(linewidth=3, foreground=BG)])

    save_fig("kpi_2_hires_by_year.png")


# =========================
# KPI 3 — Hires by Seniority
# =========================
def plot_kpi_3():
    df = run_query(Q3_SEN)
    save_csv(df, "kpi_3_hires_by_seniority.csv")

    df2 = df.sort_values("hires")

    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    bars = ax.barh(df2["seniority"], df2["hires"], color=NEON_PURPLE)
    bar_shadow(bars)

    style_axes(ax, "KPI 3 — Hires by Seniority", "Distribución de contrataciones por seniority")
    ax.set_xlabel("Hires")
    ax.set_ylabel("Seniority")

    add_bar_labels_horizontal(ax)

    save_fig("kpi_3_hires_by_seniority.png")


# =========================
# KPI 4 — Country over Years (4 countries)
# =========================
def plot_kpi_4():
    df = run_query(Q4_COUNTRY)
    save_csv(df, "kpi_4_hires_by_country_over_years.csv")

    pivot = df.pivot(index="year", columns="country", values="hires").fillna(0).sort_index()

    plt.figure(figsize=(12, 6))
    ax = plt.gca()

    for country in pivot.columns:
        color = COUNTRY_COLORS.get(country, NEON_YELLOW)
        ax.plot(pivot.index, pivot[country], marker="o", linewidth=3, markersize=7, label=country, color=color)
        # glow
        ax.lines[-1].set_path_effects([pe.Stroke(linewidth=6, foreground="#0B0F14"), pe.Normal()])

    style_axes(ax, "KPI 4 — Hires by Country over Years", "Países: USA, Brazil, Colombia, Ecuador")
    ax.set_xlabel("Year")
    ax.set_ylabel("Hires")

    leg = ax.legend(title="Country", frameon=True)
    leg.get_frame().set_facecolor(PANEL)
    leg.get_frame().set_edgecolor(GRID)

    save_fig("kpi_4_hires_by_country_over_years.png")


# =========================
# KPI 5 — Hire Rate (card)
# =========================
def plot_kpi_5():
    df = run_query(Q5_HIRERATE)
    rate = float(df.iloc[0, 0])
    save_csv(pd.DataFrame({"hire_rate_percent": [rate]}), "kpi_5_hire_rate.csv")

    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    ax.axis("off")

    ax.text(0.5, 0.70, "KPI Extra 1 — Hire Rate", ha="center", fontsize=20, weight="bold", color=TEXT)
    ax.text(0.5, 0.33, f"{rate:.2f}%", ha="center", fontsize=62, weight="bold", color=NEON_GREEN,
            path_effects=[pe.withStroke(linewidth=6, foreground="#043A2F"), pe.Normal()])

    ax.text(0.5, 0.12, "Regla: HIRED si Code ≥ 7 y Interview ≥ 7", ha="center", fontsize=10, color=MUTED)

    save_fig("kpi_extra_1_hire_rate.png")


# =========================
# KPI 6 — Avg Scores (hired only)
# =========================
def plot_kpi_6():
    df = run_query(Q6_AVG)
    save_csv(df, "kpi_6_avg_scores_hired.csv")

    code = float(df["avg_code_score"].iloc[0])
    tech = float(df["avg_interview_score"].iloc[0])

    labels = ["Avg Code Score", "Avg Interview Score"]
    values = [code, tech]

    plt.figure(figsize=(9, 5))
    ax = plt.gca()

    bars = ax.bar(labels, values, color=[NEON_CYAN, NEON_PINK])
    bar_shadow(bars)

    style_axes(ax, "KPI Extra 2 — Average Scores (Hired Only)", "Promedios de puntajes para contratados")
    ax.set_ylabel("Score")
    ax.set_ylim(0, 10)

    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width()/2, v, f"{v:.2f}", ha="center", va="bottom",
                path_effects=[pe.withStroke(linewidth=3, foreground=BG)])

    save_fig("kpi_extra_2_avg_scores_hired.png")


# =========================
# Dashboard (single image)
# =========================
def plot_dashboard():
    # Reuse KPI data (run queries again for simplicity)
    df1 = run_query(Q1_TECH).head(10).sort_values("hires")  # Top10
    df2 = run_query(Q2_YEAR)
    df3 = run_query(Q3_SEN).sort_values("hires")
    df4 = run_query(Q4_COUNTRY)
    pivot4 = df4.pivot(index="year", columns="country", values="hires").fillna(0).sort_index()
    rate = float(run_query(Q5_HIRERATE).iloc[0, 0])
    df6 = run_query(Q6_AVG)
    code = float(df6["avg_code_score"].iloc[0])
    tech = float(df6["avg_interview_score"].iloc[0])

    fig = plt.figure(figsize=(16, 9), facecolor=BG)
    gs = fig.add_gridspec(2, 3, wspace=0.25, hspace=0.35)

    # KPI1 (barh)
    ax1 = fig.add_subplot(gs[0, 0])
    bars1 = ax1.barh(df1["technology"], df1["hires"], color=NEON_CYAN)
    bar_shadow(bars1)
    style_axes(ax1, "KPI 1 — Tech (Top 10)")
    ax1.set_xlabel("Hires")
    add_bar_labels_horizontal(ax1)

    # KPI2 (line)
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.plot(df2["year"], df2["hires"], marker="o", linewidth=3, color=NEON_GREEN)
    style_axes(ax2, "KPI 2 — Hires by Year")
    ax2.set_xlabel("Year")
    ax2.set_ylabel("Hires")

    # KPI5 (card)
    ax5 = fig.add_subplot(gs[0, 2])
    ax5.axis("off")
    ax5.text(0.5, 0.65, "Hire Rate", ha="center", fontsize=18, weight="bold")
    ax5.text(0.5, 0.25, f"{rate:.2f}%", ha="center", fontsize=46, weight="bold", color=NEON_GREEN)

    # KPI3 (seniority)
    ax3 = fig.add_subplot(gs[1, 0])
    bars3 = ax3.barh(df3["seniority"], df3["hires"], color=NEON_PURPLE)
    bar_shadow(bars3)
    style_axes(ax3, "KPI 3 — Seniority")
    ax3.set_xlabel("Hires")
    add_bar_labels_horizontal(ax3)

    # KPI4 (countries)
    ax4 = fig.add_subplot(gs[1, 1])
    for country in pivot4.columns:
        ax4.plot(pivot4.index, pivot4[country], marker="o", linewidth=2.8,
                 label=country, color=COUNTRY_COLORS.get(country, NEON_YELLOW))
    style_axes(ax4, "KPI 4 — Country over Years")
    ax4.set_xlabel("Year")
    ax4.set_ylabel("Hires")
    leg = ax4.legend(frameon=True, fontsize=9)
    leg.get_frame().set_facecolor(PANEL)
    leg.get_frame().set_edgecolor(GRID)

    # KPI6 (avg scores)
    ax6 = fig.add_subplot(gs[1, 2])
    bars6 = ax6.bar(["Avg Code", "Avg Interview"], [code, tech], color=[NEON_CYAN, NEON_PINK])
    bar_shadow(bars6)
    style_axes(ax6, "KPI 6 — Avg Scores (Hired)")
    ax6.set_ylim(0, 10)
    for b, v in zip(bars6, [code, tech]):
        ax6.text(b.get_x() + b.get_width()/2, v, f"{v:.2f}", ha="center", va="bottom",
                 path_effects=[pe.withStroke(linewidth=3, foreground=BG)])

    # Big title
    fig.suptitle("ETL Workshop — KPI Dashboard (DW: PostgreSQL)", fontsize=20, weight="bold", color=TEXT, y=0.98)

    path = os.path.join(OUTPUT_DIR, "dashboard_kpis.png")
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(path, dpi=220, facecolor=BG)
    plt.close(fig)
    print(f"Saved: {path}")


def main():
    ensure_dirs()

    plot_kpi_1()
    plot_kpi_2()
    plot_kpi_3()
    plot_kpi_4()
    plot_kpi_5()
    plot_kpi_6()
    plot_dashboard()

    print("\n All premium charts + dashboard generated in /visualizations")
    print("All KPI tables exported to /data/processed")


if __name__ == "__main__":
    main()