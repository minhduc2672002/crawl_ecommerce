import os
import random
import logging
import time
import requests
from bs4 import BeautifulSoup, Tag
from urllib.parse import urlparse
import pandas as pd
import re
import unicodedata
import string
from datetime import datetime
import argparse
class WebCrawler:
    def __init__(self, base_url : str | None = None, output_dir="."):
        self.base_url = base_url
        self.domain = self.extract_domain(base_url)
        self.output_dir = os.path.join(output_dir, self.domain)
        self.checkpoint_path = os.path.join(output_dir,f"{self.domain}__checkpoint.csv")

    def polite_request(self, url: str, time_out: int = 60):
        """
        Sends a polite GET request to the given URL with randomized user-agent headers.

        Args:
            url (str): The URL to send the request to.
            time_out (int): The maximum time to keep retrying, in seconds (default is 60).

        Returns:
            Response or None: The response object if successful, or None if the request fails after retries.
        """
        if url is None:
            return None

        time_count = 0
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.81",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0"
        ]

        while time_count < time_out:
            try:
                response = requests.get(
                    url,
                    headers={'User-Agent': random.choice(user_agents)},
                    timeout=5
                )
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError:
                logging.error(f"HTTP Error occurred for URL: {url}")
            except requests.exceptions.ConnectionError:
                logging.error("Network disconnected while trying to reach URL: %s", url)
            except requests.exceptions.RequestException:
                logging.error(f"Request failed for URL: {url}")

            time_count += 5
            time.sleep(5)

        logging.info(f"Request to URL {url} failed after {time_out} seconds.")
        return None


    def get_product_sitemap(self,url: str) -> str:
        """
        Fetch the product sitemap URL from the main sitemap of a given website.

        Args:
            url (str): The base URL of the website.

        Returns:
            str: The URL of the product sitemap if found, otherwise None.
        """
        url_sitemap = f"{url}/sitemap.xml"
        product_sitemap = None
        response = self.polite_request(url_sitemap)

        if response and response.status_code == 200:
            logging.info("Request succeeded for URL: %s", url)
            soup = BeautifulSoup(response.content, 'xml')
            loc_tags = soup.find_all('loc')

            for loc in loc_tags:
                location = loc.get_text(strip=True)
                if "product" in location:
                    product_sitemap = location
                    break
        return product_sitemap

    def get_product_urls(self, product_sitemap: str) -> list:
        """
        Fetch product URLs from a given product sitemap.

        Args:
            product_sitemap (str): The URL of the product sitemap.

        Returns:
            list: A list of product URLs extracted from the sitemap.
        """
        import logging
        from bs4 import BeautifulSoup

        list_url = []
        response = self.polite_request(product_sitemap)

        if response and response.status_code == 200:
            logging.info("Request succeeded for URL: %s", product_sitemap)
            soup = BeautifulSoup(response.content, 'xml')
            urls = soup.find_all('url')

            for url in urls:
                loc_tag = url.find('loc')
                if loc_tag:
                    list_url.append(loc_tag.get_text(strip=True))

        return list_url
    
    def get_product_title(self, soup : BeautifulSoup) -> str:
        """
        Get and process the title text by:
        1. Removing extra spaces
        2. Removing Vietnamese diacritics
        3. Removing punctuation
        4. Replacing spaces with hyphens

        Args:
            soup (BeautifulSoup): Input soup to process

        Returns:
            str: title text
        """
        title = soup.title.string

        text = re.sub(r'\s+', ' ', title).strip()
        normalized_text = unicodedata.normalize('NFD', text)
        text = ''.join(char for char in normalized_text if unicodedata.category(char) != 'Mn')
        text = text.replace('đ', 'd').replace('Đ', 'D')
        text = re.sub(r'\|.*', '', text)
        text = re.sub(r'\[.*?\]', '', text)
        text = text.lower()
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        processed_text = '-'.join(segment.strip() for segment in text.split(' ') if segment.strip())
        
        return processed_text
    
    
    def get_product_infor_html(self, soup : BeautifulSoup) -> Tag:
        header = soup.find("header")
        footer = soup.find("footer")

        for tag in soup.find_all(["button", "input","script"]):
            tag.decompose()

        extracted_content = soup.new_tag("div")
        if header and footer:
            for sibling in header.find_next_siblings():
                if sibling == footer:
                    break
                extracted_content.append(sibling)

        return extracted_content
    
    def get_product_infor_text(self, product_html : Tag) -> str:
        import re
        text = product_html.get_text()
        cleaned_text = re.sub(r'\n{2,}', '\n', text)
        cleaned_text = re.sub(r'\n\t+', '', cleaned_text)
        cleaned_text = re.sub(r'\t', ' ', cleaned_text)

        return cleaned_text
    
    def save_product_info(self, product_title : str, product_info : str,path : str = "."):
        if product_title == "":
            return
        with open(f'{path}/{product_title}.txt','w+') as file:
            file.write(product_info)
    

    def get_product_info(self, url_product : str) -> str:
        product_info_text = ""
        response = self.polite_request(url_product)
        if response and response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            product_info_html = self.get_product_infor_html(soup)
            product_info_text = self.get_product_infor_text(product_info_html)

        return product_info_text

    def extract_domain(self, url: str):
        from urllib.parse import urlparse
        """
        Extract domain from URL.
        """
        try:
            parsed_url = urlparse(url)
            # Lấy phần domain từ netloc
            domain = parsed_url.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception as e:
            print(f"Error parsing URL {url}: {e}")
            return None


    def load_check_point(self,check_point_path : str) -> set:
        if os.path.exists(check_point_path):
            check_point_df = pd.read_csv(check_point_path,header=None)
            return set(check_point_df[0])
        return set()
    
    def save_check_point(self, check_point_path : str,checkpoint : set) :
        df = pd.DataFrame(list(checkpoint))
        df.to_csv(check_point_path,header=False,index=False)

    def crawl(self):
        if self.domain:
            if not os.path.exists(self.output_dir):
                os.mkdir(self.output_dir)

            folder_name = self.output_dir
            product_sitemap = self.get_product_sitemap(self.base_url)
            list_url = self.get_product_urls(product_sitemap)
            checkpoint_list = self.load_check_point(self.checkpoint_path)
            try:
                for url in list_url:
                    if url in checkpoint_list:
                        continue
                    product_title, product_info_text = self.get_product_info(url)
                    self.save_product_info(product_title,product_info_text,path=folder_name)
                    checkpoint_list.add(url)
            except Exception as e:
                print(e)
            finally:
                self.save_check_point(self.checkpoint_path,checkpoint_list)

     
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl a website")
    parser.add_argument("url", type=str, help="The URL of the website to crawl")

    args = parser.parse_args()

    crawler = WebCrawler(args.url)
    crawler.crawl()