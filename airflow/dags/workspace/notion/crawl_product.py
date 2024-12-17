import requests
import json
from dotenv import load_dotenv
import os
import sys
from notion_client  import NotionClient
from crawler import WebCrawler
import concurrent.futures
import queue
# Thêm thư mục cha vào sys.path
sys.path.append(os.path.abspath("../"))
load_dotenv()



from bs4 import BeautifulSoup
import requests
from datetime import datetime

def convert_to_timestamp(time_string):
    # Các định dạng thời gian có thể có
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # Ví dụ: 2024-12-12T10:03:00.781Z
        "%Y-%m-%dT%H:%M:%SZ",     # Ví dụ: 2024-12-12T10:03:00Z
        "%Y-%m-%d %H:%M:%S",       # Ví dụ: 2024-12-12 10:03:00
        "%d/%m/%Y %H:%M:%S",       # Ví dụ: 12/12/2024 10:03:00
        "%m/%d/%Y %H:%M:%S",       # Ví dụ: 12/12/2024 10:03:00
        "%Y-%m-%d",                # Ví dụ: 2024-12-12
        "%d/%m/%Y"                 # Ví dụ: 12/12/2024
    ]
    
    # Thử qua các định dạng để phân tích chuỗi thời gian
    for fmt in formats:
        try:
            # Chuyển đổi thành datetime và sau đó sang timestamp
            dt = datetime.strptime(time_string, fmt)
            return int(dt.timestamp())  # Trả về timestamp
        except ValueError:
            continue  # Tiếp tục nếu không thành công với định dạng này
    
    raise ValueError(f"Không thể phân tích chuỗi thời gian: {time_string}")

def fetch_data_from_sitemap(xml_url: str):
    try:
        response = requests.get(xml_url)
        response.raise_for_status()  
        xml_content = response.text 
        
        soup = BeautifulSoup(xml_content, 'xml')

        urls = []
        for url_tag in soup.find_all('url'):
            url_data = {}
            
            for child in url_tag.find_all(recursive=False):
                if child.name != 'image' and child.name != "changefreq": 
                    url_data[child.name] = child.text.strip() if child.text else None

            image_tag = url_tag.find('image:image')
            if image_tag:
                image_title = image_tag.find('image:title')
                
                if image_title:
                    url_data['title'] = image_title.text.strip()

            if url_data.get("lastmod",None):
                lastmod= convert_to_timestamp(url_data.get("lastmod",None))
                url_data["lastmod"] = lastmod
                urls.append(url_data)
        return urls
    except Exception as e:
        print(f"Error: {e}")
        return []



def get_meta_data(pages: dict,num_pages=None) -> dict:
    data = dict()
    if pages is not None:
        for page in pages:
            props = page["properties"]
            # print(props)
            page_name = props["page_name"]["title"][0]["text"]["content"]
            page_id = props["page_id"]["rich_text"][0]["text"]["content"]
            # print(page_id)
            data[page_name] = page_id
    return data


def split_text_into_parts(text, max_length=2000):
    """
    Splits the text into smaller chunks, each no longer than `max_length` characters.

    Args:
    - text (str): The long text to split.
    - max_length (int): The maximum length of each chunk (default is 2000 characters).

    Returns:
    - list: A list of text segments, each with length ≤ `max_length`.
    """
    # List to store the text chunks
    chunks = []
    
    # Split the text into chunks of max_length
    for i in range(0, len(text), max_length):
        chunks.append(text[i:i + max_length])
    
    return chunks
def crawl_and_store_notion(originnal_data : dict, page_id : str):
        data = originnal_data
        product_info = crawler.get_product_info(data['loc'])
        chunks = split_text_into_parts(product_info)
        new_page_reponse = client.create_new_page(page_id,data['title'])
        if new_page_reponse.status_code != 200:
            print(new_page_reponse.json())
        page_id = new_page_reponse.json().get("id",None)
        for chunk in chunks:
            if page_id is not None:
                client.add_code_block(page_id,chunk)
        data['page_id'] = page_id
        return page_id,data




if __name__ == "__main__":
    client = NotionClient(notion_token=os.getenv("NOTION_TOKEN"),
                          root_id=os.getenv("DATABASE_ID"))
    crawler = WebCrawler("")

    meta_data = dict()
    task_queue = queue.Queue()
    PAGE_ID = "15f6739fbff1800f9dacd502f1638f00"

    sitemap_products = fetch_data_from_sitemap("https://shop.joygarden.vn/sitemap_products_1.xml")

    for data in sitemap_products:
        task_queue.put(data)


    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        # Tạo các threads, mỗi thread sẽ xử lý các nhiệm vụ từ queue
        while not task_queue.empty():
            data = data = task_queue.get()
            future = executor.submit(crawl_and_store_notion, data, PAGE_ID)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            page_id,data = future.result()
            if page_id is not None:
                meta_data[data['loc']] = data
    
    with open("meta_data.json","w+") as file:
        file.write(json.dumps(meta_data,ensure_ascii=False,indent=4))
    