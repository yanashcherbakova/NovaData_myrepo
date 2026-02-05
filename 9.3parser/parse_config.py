import requests
from bs4 import BeautifulSoup
import time
import csv

#HttpClient -- response
#BookBowler - get soup / get data
#CatalogePager  -- next page

## csv structure:
## UPC, BOOK_TITLE / price / , rating, / In stock (0, 1, 2...), 

def write_to_csv(parsed_info):
    now = time.strftime("%m%d_%H%M")
    filename = f"raw_result_{now}.csv"

    try:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=parsed_info[0].keys())
            writer.writeheader()
            writer.writerows(parsed_info)
        print(f"CSV correctly written --> {filename}")
        return filename
    except:
        print("Error while csv creation")
        return False

class HttpClient:
    def __init__(self, headers, max_retries = 3, request_delay = 2, timeout = 15):
        self.headers=headers
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.max_retries = max_retries
        self.request_delay = request_delay
        self.timeout = timeout

    def get_response(self, url):
        attempt = 0
        while attempt < self.max_retries:
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.encoding = "utf-8"
                return response
            except requests.exceptions.RequestException as e:
                print(f"...Request error for {url}: {e}, attempt {attempt}")
                attempt += 1
                time.sleep(self.request_delay)
        print(f"...Finally failed to reach page -- {url}")
        return None
    

class CatalogePager:
    def get_next_page_url(self, url):
        if not url.endswith(".html"):
            print("Not an html")
            return None
        
        base, tail = url.rsplit("-", 1)
        number_part = tail.replace(".html", "")

        if not number_part.isdigit():
            print("Failed to get number of a page in url")

        next_page = int(number_part) + 1
        print(f"Next page # {next_page}")
        return f"{base}-{next_page}.html"


class BookBowler:
    def __init__(self, http: HttpClient, pager: CatalogePager, base_url):
        self.http = http
        self.pager = pager
        self.base_url = base_url
        self.rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4,"Five": 5}

    def parse_book_page(self, book_resp):
        if book_resp is None:
            return None
        
        Page_soup = BeautifulSoup(book_resp.text, "html.parser")

        #get title (BOOK PAGE)
        title = Page_soup.find("h1").text.strip()

        #get price (BOOK PAGE)
        th_price = Page_soup.find("th", string="Price (excl. tax)")
        price_text = th_price.find_next_sibling("td").get_text(strip=True) if th_price else None

        price = float(price_text.replace("£", "")) if price_text else None

        #get rating (BOOK PAGE)
        rating_class = Page_soup.find("p", class_="star-rating")
        rating_word = rating_class["class"][1]
        rating = self.rating_map[rating_word] if rating_word else None

        #get in stock info (BOOK PAGE)
        availability = Page_soup.find("th", string="Availability")
        stock = 0
        stock_string = availability.find_next_sibling("td").get_text(strip=True) if availability else None
        digits = [c for c in stock_string if c.isdigit()]
        if digits:
            stock = int("".join(digits))

        #get UPC (BOOK PAGE)
        th = Page_soup.find("th", string="UPC")
        upc = th.find_next_sibling("td").get_text(strip=True) if th else None

        return title, price, rating, stock, upc

    def parse_cataloge_page(self, soup):
        on_site = soup.find_all("article", class_="product_pod")
        one_page = []
        for article in on_site:

            #get link to BOOK PAGE
            h3 = article.find("h3")
            a = h3.find("a")
            link = a["href"] 
            book_link = self.base_url + link
 
            #get book_page soup
            book_resp = self.http.get_response(book_link)
            if book_resp.status_code == 200:
                print("I opened book page!")
                title, price, rating, stock, upc = self.parse_book_page(book_resp)
            else:
                #get info about book from cataloge page (not full info)

                #get title (CATALOGUE PAGE)
                title = article.h3.a["title"] 

                #get price (CATALOGUE PAGE)
                price = None
                price_tag = article.find("p", class_="price_color")

                if price_tag:
                    price_text = price_tag.text.strip().replace("£", "")
                    price = float(price_text)

                #get rating (CATALOGUE PAGE)
                rating_class = article.find("p", class_="star-rating")
                rating_word = rating_class["class"][1]
                rating = self.rating_map[rating_word] if rating_word else None

                #missed info on CATALOGUE PAGE
                upc = None
                stock = None
            
            one_book = {"upc" : upc, "book_title" : title, "price" : price, "rating" : rating, "in_stock" : stock}
            one_page.append(one_book)

        return one_page

    def crawl(self, url):
        parsed_info = []

        while True:
            resp = self.http.get_response(url)

            if resp.status_code == 404:
                print("End of pages --> Stop")
                break
            elif resp.status_code == 200:
                print("I opened cataloge page")

                soup = BeautifulSoup(resp.text, "html.parser")
                parsed_info.extend(self.parse_cataloge_page(soup))

                url = self.pager.get_next_page_url(url)
                if url is None:
                    break
            else:
                print(f"Other response status {resp.status_code}--> Stop")
                break
        return parsed_info
                


                

    
    