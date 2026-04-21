from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

BASE_DIR = Path("/opt/spark/work-dir")
DATA_DIR = BASE_DIR / "data" / "raw"

DB_USER = "flowtrack"
DB_PASSWORD = "flowtrack"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "flowtrack"

TABLE_FILE_MAP = {
    "warehouses": DATA_DIR / "warehouses.csv",
    "carriers": DATA_DIR / "carriers.csv",
    "routes": DATA_DIR / "routes.csv",
    "shipments": DATA_DIR / "shipments.csv",
}


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def truncate_table(engine, schema: str, table: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE"))


def load_csv_to_table(engine, table_name: str, csv_path: Path, schema: str = "raw"):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    if table_name == "shipments":
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["promised_delivery_at"] = pd.to_datetime(df["promised_delivery_at"])

    print(f"Loading {csv_path.name} -> {schema}.{table_name} ({len(df)} rows)")

    truncate_table(engine, schema, table_name)

    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"Finished loading {schema}.{table_name}")


def validate_row_counts(engine, schema: str = "raw"):
    with engine.begin() as conn:
        for table_name in TABLE_FILE_MAP:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            ).scalar()
            print(f"{schema}.{table_name}: {result} rows")


def main():
    engine = get_engine()

    for table_name, csv_path in TABLE_FILE_MAP.items():
        load_csv_to_table(engine, table_name, csv_path)

    print("\nValidation:")
    validate_row_counts(engine)


if __name__ == "__main__":
    main()
