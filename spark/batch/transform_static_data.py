import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "flowtrack"
DB_PASSWORD = "flowtrack"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "flowtrack"


def get_engine():
    conn_str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(conn_str)


def truncate_table(engine, schema: str, table: str):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema}.{table} CASCADE"))


def read_table(engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, engine)


def write_table(engine, df: pd.DataFrame, schema: str, table_name: str):
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


def transform_warehouses(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["warehouse_name"] = df["warehouse_name"].str.strip()
    df["city"] = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.title()
    df["warehouse_type"] = df["warehouse_type"].str.strip().str.lower()
    df["capacity"] = df["capacity"].fillna(0).astype(int)
    return df.drop_duplicates(subset=["warehouse_id"])


def transform_carriers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["carrier_name"] = df["carrier_name"].str.strip()
    df["carrier_type"] = df["carrier_type"].str.strip().str.lower()
    df["service_level"] = df["service_level"].str.strip().str.lower()
    return df.drop_duplicates(subset=["carrier_id"])


def transform_routes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce").fillna(0)
    df["estimated_duration_hours"] = pd.to_numeric(
        df["estimated_duration_hours"], errors="coerce"
    ).fillna(0)
    return df.drop_duplicates(subset=["route_id"])


def transform_shipments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df["promised_delivery_at"] = pd.to_datetime(df["promised_delivery_at"], errors="coerce")
    df["shipment_status"] = df["shipment_status"].str.strip().str.upper()

    priority_carriers = {"C001"}
    df["is_priority"] = df["carrier_id"].isin(priority_carriers)

    df = df.drop_duplicates(subset=["shipment_id"])
    return df


def validate_counts(engine):
    queries = {
        "stg_warehouses": "SELECT COUNT(*) FROM staging.stg_warehouses",
        "stg_carriers": "SELECT COUNT(*) FROM staging.stg_carriers",
        "stg_routes": "SELECT COUNT(*) FROM staging.stg_routes",
        "stg_shipments": "SELECT COUNT(*) FROM staging.stg_shipments",
    }

    with engine.begin() as conn:
        for table_name, query in queries.items():
            count = conn.execute(text(query)).scalar()
            print(f"{table_name}: {count} rows")


def main():
    engine = get_engine()

    raw_warehouses = read_table(engine, "SELECT * FROM raw.warehouses")
    raw_carriers = read_table(engine, "SELECT * FROM raw.carriers")
    raw_routes = read_table(engine, "SELECT * FROM raw.routes")
    raw_shipments = read_table(engine, "SELECT * FROM raw.shipments")

    stg_warehouses = transform_warehouses(raw_warehouses)
    stg_carriers = transform_carriers(raw_carriers)
    stg_routes = transform_routes(raw_routes)
    stg_shipments = transform_shipments(raw_shipments)

    write_table(engine, stg_warehouses, "staging", "stg_warehouses")
    print("Finished loading staging.stg_warehouses")

    write_table(engine, stg_carriers, "staging", "stg_carriers")
    print("Finished loading staging.stg_carriers")

    write_table(engine, stg_routes, "staging", "stg_routes")
    print("Finished loading staging.stg_routes")

    write_table(engine, stg_shipments, "staging", "stg_shipments")
    print("Finished loading staging.stg_shipments")

    print("\nValidation:")
    validate_counts(engine)


if __name__ == "__main__":
    main()
