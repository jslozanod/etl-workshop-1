from extract import extract
from transform import transform
from load import load_to_dw

def main():
    csv_path = "data/raw/candidates.csv"

    print(" Extracting...")
    raw = extract(csv_path)

    print(" Transforming...")
    dim_candidate, dim_country, dim_date, dim_seniority, dim_technology, fact_raw = transform(raw)

    print(" Loading to PostgreSQL...")
    load_to_dw(dim_candidate, dim_country, dim_date, dim_seniority, dim_technology, fact_raw)

    print("DONE! Data loaded into etl_dw")

if __name__ == "__main__":
    main()