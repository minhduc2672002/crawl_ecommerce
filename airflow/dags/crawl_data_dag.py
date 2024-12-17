from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import json
from workspace.crawler import WebCrawler

def crawl_data():
    with open('/opt/airflow/dags/urls.json', 'r') as file:
        urls = json.load(file)
    print(urls)
    
    # for url in list(urls):
    #     crawler = WebCrawler(url,"./../data")
    #     crawler.crawl() 


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 16),
    }

with DAG('crawl_data',
         default_args = default_args,
         schedule_interval= None,
         catchup=False
        ) as dag:
    
    crawl_data_task = PythonOperator(
        task_id = "crawl_data_task",
        python_callable=crawl_data
    )