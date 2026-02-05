from parse_config import write_to_csv, HttpClient, CatalogePager, BookBowler
from process_data import read_csv, clean_books_df, get_volumes_and_single_books
from sqlalchemy import create_engine
from db_connection import create_tables, load_data

#HttpClient -- response
#BookBowler - get soup / get data
#CatalogePager  -- next page

## csv structure:
## UPC, BOOK_TITLE / price / , rating, / In stock (0, 1, 2...), 

def main():
    # === parse ... parse ... parse ===
    url = "https://books.toscrape.com/catalogue/page-1.html"
    base_url = "https://books.toscrape.com/catalogue/"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    } 

    http = HttpClient(HEADERS)
    pager = CatalogePager()

    client = BookBowler(http, pager, base_url)

    parsed_info = client.crawl(url)

    filename = write_to_csv(parsed_info)

    # === process ... process ... prosecc ===
    # we'll derive 2 DFs ---> First: df with single books
    #                    ---> Second: df with volumes

    df = read_csv(filename)
    df = clean_books_df(df)

    single_df, volume_df = get_volumes_and_single_books(df)

    # === upload ... upload ... upload ===

    engine = create_engine("postgresql+psycopg2://yana:1147@localhost:5432/bookstore")

    create_tables(engine)
    load_data(engine, single_df, volume_df)


if __name__ == "__main__":
    main()