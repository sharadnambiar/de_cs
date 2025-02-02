from sqlalchemy import create_engine, text

DB_CONN = "postgresql+psycopg2://postgres:postgres@localhost:5432/iff"
engine = create_engine(DB_CONN)

with engine.connect() as conn:
    result = conn.execute(text("SELECT schema_name FROM information_schema.schemata"))
    print([row[0] for row in result])
