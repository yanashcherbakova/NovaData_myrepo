from sqlalchemy import text

def create_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS single_books_catalog (
                upc text PRIMARY KEY,
                book_title text NOT NULL,
                price numeric(10,2),
                rating smallint,
                in_stock smallint
            );
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS volume_books_catalog (
                upc text PRIMARY KEY,
                book_title text NOT NULL,
                price numeric(10,2),
                rating smallint,
                in_stock smallint
            );
        """))
    print("DB: Tables created")
    return True

def load_data(engine, single_df, volume_df):
    try:
        with engine.begin() as conn:
            single_df.to_sql("single_books_catalog", conn, if_exists="append", index=False)
            volume_df.to_sql("volume_books_catalog", conn, if_exists="append", index=False)
            print("Ready in DB tables")
    except Exception:
        print("Failed to load data (rolled back)")
        raise