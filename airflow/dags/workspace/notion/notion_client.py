import requests
import time
import json
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
load_dotenv()

DATABASES_ENDPOINT = "https://api.notion.com/v1/databases"
PAGES_ENDPOINT = "https://api.notion.com/v1/pages"
BLOCKS_ENDPOINT = "https://api.notion.com/v1/blocks"



session = requests.Session()

# Cấu hình Retry
retry_strategy = Retry(
    total=3,  # Tổng số lần thử lại
    backoff_factor=3,  # Thời gian chờ giữa các lần thử lại (exponential backoff)
    status_forcelist=[500, 502, 503, 504,404,409,400],  # Các mã trạng thái HTTP sẽ thử lại
    allowed_methods=["HEAD", "GET", "POST"]  # Các phương thức HTTP được áp dụng retry
)

# Áp dụng HTTPAdapter với retry strategy cho session
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

class NotionClient:
    def __init__(self,notion_token : str, root_id: str) -> None:
        self.notion_token = notion_token
        self.root_id = root_id


    @property
    def headers(self):
        return {
            "Authorization": f"Bearer {self.notion_token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }
    
    def get_rows(self,database_id: str,num_rows=None) ->dict:
        """
        If num_rows is None, get all rows, otherwise just the defined number.
        """
        url = f"{DATABASES_ENDPOINT}/{database_id}/query"

        get_all = num_rows is None
        page_size = 100 if get_all else num_rows

        payload = {"page_size": page_size}
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            data = response.json()

            results = data["results"]
            while data["has_more"] and get_all:
                payload = {"page_size": page_size, "start_cursor": data["next_cursor"]}
                url = f"https://api.notion.com/v1/databases/{database_id}/query"
                response = session.post(url, json=payload, headers=self.headers)
                data = response.json()
                results.extend(data["results"])

            return results
        except:
            return None
    

    def insert_row(self,database_id : str, data: dict) -> bool:
        """
            Creates a new row in the specified Notion database.

            Data Example:
            {
                "page_name": {"title": [{"text": {"content": "Shoppe"}}]},
                "page_id": {"rich_text": [{"text": {"content": "haha"}}]},
            }

        """
        payload = {"parent": {"database_id": database_id}, "properties": data}

        res = requests.post(PAGES_ENDPOINT, headers=self.headers, json=payload)
        
        return res.status_code  == 200
    
    def create_new_page(self, parrent_page_id : str,subpage_name: str ):
        """
            create new page in Notion
        """
        # time.sleep(4)
        data = {
            "parent": {
                "type": "page_id",
                "page_id": parrent_page_id
            },
            "properties": {
                "title": [
                    {
                        "type": "text",
                        "text": {
                            "content": subpage_name
                        }
                    }
                ]
            }
        }

        # Gửi yêu cầu POST để tạo page mới
        response = session.post(PAGES_ENDPOINT, headers=self.headers, data=json.dumps(data))
        return response
    
    def add_text_block(self,page_id : str, content : str):
        """
            Insert text into block in Notion
        """
        url_add_block = f"{BLOCKS_ENDPOINT}/{page_id}/children"
        data_add_text_block = {
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": content,
                                    "link": None
                                }
                            }
                        ],
                        "color": "default"
                    }
                }
            ]
        }

        # Gửi yêu cầu POST để thêm khối văn bản
        response = requests.patch(url_add_block, headers=self.headers, data=json.dumps(data_add_text_block))

        # Kiểm tra kết quả
        if response.status_code == 200:
            print("Text block added successfully!")
            print("Response:", response.json())
        else:
            print("Failed to add text block!")
            print("Status Code:", response.status_code)
            print("Response:", response.text)
    def add_code_block(self,page_id : str, content : str):
        """
            Insert text into block in Notion
        """
        url_add_block = f"{BLOCKS_ENDPOINT}/{page_id}/children"
        payload = {
            "children": [
                {
                    "object": "block",
                    "type": "code",
                    "code": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": content,
                                }
                            }
                        ],
                        "language": "plain text"  # Ngôn ngữ của code (vd: python, javascript, sql, ...)
                    }
                }
            ]
        }
        # Gửi yêu cầu POST để thêm khối văn bản
        response = requests.patch(
            url_add_block, 
            headers=self.headers, 
            json=payload
        )

        # Kiểm tra kết quả
        if response.status_code != 200:
            print("Failed to add code block!")
            print("Status Code:", response.status_code)
            print("Response:", response.text)


    
    def get_code_blocks(self, page_id: str):
        notion_api_url = f"{BLOCKS_ENDPOINT}/{page_id}/children"
        response = session.get(notion_api_url, headers=self.headers)
        
        if response.status_code == 200:
            blocks = response.json().get("results", [])
            code_blocks = []
            
            # Lọc các code block
            for block in blocks:
                if block["type"] == "code":
                    block_id = block.get("id","unknow")
                    code_content = "".join(
                        [rich_text["text"]["content"] for rich_text in block["code"]["rich_text"]]
                    )
                    # code_language = block["code"].get("language", "unknown")
                    code_blocks.append({"content": code_content,
                                        "block_id":block_id
                                        })
            
            return code_blocks
        else:
            # Xử lý lỗi
            print("Lỗi khi gọi API:", response.json())
            return []
        

    def clear_code_blocks(self,page_id :str):
        url = f"{BLOCKS_ENDPOINT}/{page_id}/children"
        response = session.get(url, headers=self.headers)
        if response.status_code == 200:
            blocks = response.json()["results"]  # List of blocks on the page
            
            # Iterate through the blocks and delete code blocks
            for block in blocks:
                if block["type"] == "code":  # Check if the block is a code block
                    block_id = block["id"]
                    delete_url = f"{BLOCKS_ENDPOINT}/{block_id}"
                    
                    # Send DELETE request to remove the block
                    delete_response = requests.delete(delete_url, headers=self.headers)
                    
                    if delete_response.status_code == 200:
                        print(f"Code block {block_id} deleted successfully.")
                    else:
                        print(f"Failed to delete block {block_id}. Status code: {delete_response.status_code}, Response: {delete_response.text}")
        else:
            print(f"Failed to fetch blocks. Status code: {response.status_code}, Response: {response.text}")





def fetch_data_from_sitemap(xml_url):
    try:
        # Gửi yêu cầu HTTP để tải sitemap XML
        response = session.get(xml_url)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        xml_content = response.text  # Nội dung XML

        # Phân tích cú pháp XML với BeautifulSoup
        soup = BeautifulSoup(xml_content, 'xml')

        # Lấy tất cả các thẻ <url>
        urls = []
        for url_tag in soup.find_all('url'):
            url_data = {}
            # Lấy các thẻ con bên trong <url>, ví dụ <loc>, <lastmod>, ...
            for child in url_tag.find_all(recursive=False):
                url_data[child.name] = child.text.strip() if child.text else None
            urls.append(url_data)

        return urls 
    except Exception as e:
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
if __name__ == "__main__":
    # xml_url = "https://shop.joygarden.vn/sitemap_products_1.xml"
    # data = fetch_data_from_sitemap(xml_url)
    
    client = NotionClient(notion_token=os.getenv("NOTION_TOKEN"))
    pages = client.get_rows("15c6739fbff180d58d8dd3c3978aaf68")
    rs = get_meta_data(pages)
    print(rs)
