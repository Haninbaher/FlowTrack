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


def read_events(engine) -> pd.DataFrame:
    query = "SELECT * FROM raw.shipment_events"
    return pd.read_sql(query, engine)


def transform_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["estimated_arrival"] = pd.to_datetime(df["estimated_arrival"], errors="coerce")
    df["actual_arrival"] = pd.to_datetime(df["actual_arrival"], errors="coerce")
    df["ingestion_time"] = pd.to_datetime(df["ingestion_time"], errors="coerce")

    df["event_type"] = df["event_type"].str.strip().str.lower()
    df["status"] = df["status"].str.strip().str.upper()
    df["location"] = df["location"].fillna("Unknown").str.strip()

    df["delay_reason"] = df["delay_reason"].fillna("")
    df["is_delayed_event"] = df["event_type"].eq("delayed")

    df = df.drop_duplicates(subset=["event_id"])

    return df


def write_events(engine, df: pd.DataFrame):
    truncate_table(engine, "staging", "stg_shipment_events")

    df.to_sql(
        name="stg_shipment_events",
        con=engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def validate(engine):
    with engine.begin() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM staging.stg_shipment_events")
        ).scalar()
        print(f"staging.stg_shipment_events: {count} rows")


def main():
    engine = get_engine()
    df = read_events(engine)
    transformed = transform_events(df)
    write_events(engine, transformed)
    validate(engine)


if __name__ == "__main__":
    main()
