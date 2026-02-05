import pandas as pd

VOLUMES_TRACE = ["volume", "vol"]

def has_volume_mark(title):
    if any(word in title.lower() for word in VOLUMES_TRACE):
        return True
    elif "#" in title:
        i = title.find("#")
        tail = title[i+1:]
        tail = tail.lstrip()
        return len(tail) > 0 and tail[0].isdigit()
    else:
        return False

def read_csv(filename):
    try:
        return pd.read_csv(filename, encoding="utf-8", header=0)
    except Exception as e:
        print(f"Error while reading csv: {e}")
        return None

def clean_books_df(df):
    if df.empty:
        print("DF is empty")
    
    print("Processing...")
    df = df.dropna(subset=["book_title", "price"])

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["in_stock"] = pd.to_numeric(df["in_stock"], errors="coerce")

    df = df[(df["price"] >= 0) & (df["rating"].between(0, 5))]

    df = df.drop_duplicates(subset=["upc"])

    return df

def get_volumes_and_single_books(df):
    mask = df["book_title"].apply(has_volume_mark)

    v_filtered = df[mask]
    v_sorted = v_filtered.sort_values(by="book_title")

    v_df = v_sorted.reset_index(drop=True)
    print("Volume books df is ready")

    single_filterd = df[~mask]
    single_sorted = single_filterd.sort_values(by="book_title")

    single_df = single_sorted.reset_index(drop=True)
    print("Single books df is ready")

    return single_df, v_df
