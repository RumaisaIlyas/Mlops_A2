from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Rumaisa_scraping_dag',
    default_args=default_args,
    description='A DAG to scrape webpages, clean data, and save to CSV',
    schedule_interval='@daily',  # Run daily
)

def scrape_webpage(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    extracted_links = []
    extracted_titles = []
    extracted_descriptions = []

    for anchor_tag in soup.find_all('a'):
        href = anchor_tag.get('href')
        if href and href.startswith('http'):
            extracted_links.append(href)
            title = anchor_tag.get_text(strip=True)
            extracted_titles.append(title)
            description = anchor_tag.find_next_sibling(string=True)
            if description and len(description.strip()) > 0:
                extracted_descriptions.append(description.strip())
            else:
                extracted_descriptions.append("")

    return extracted_titles, extracted_descriptions

def clean_text(text):
    cleaned_text = re.sub(r'[^a-zA-Z\s]', '', text)
    cleaned_text = cleaned_text.lower()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

def save_to_csv(filename, data):
    with open(filename, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Website', 'Title', 'Description'])
        writer.writerows(data)

def scrape_and_clean_data(website, **kwargs):
    url_map = {
        'Dawn': 'https://www.dawn.com/',
        'BBC': 'https://www.bbc.com/'
    }
    titles, descriptions = scrape_webpage(url_map[website])
    cleaned_titles = [clean_text(title) for title in titles]
    cleaned_descriptions = [clean_text(description) for description in descriptions]
    data_with_website = [(website, title, description) for title, description in zip(cleaned_titles, cleaned_descriptions)]
    save_to_csv(f'{website.lower()}_data.csv', data_with_website)

scrape_and_clean_dawn_task = PythonOperator(
    task_id='scrape_and_clean_dawn',
    python_callable=scrape_and_clean_data,
    op_args=['Dawn'],
    provide_context=True,
    dag=dag,
)

scrape_and_clean_bbc_task = PythonOperator(
    task_id='scrape_and_clean_bbc',
    python_callable=scrape_and_clean_data,
    op_args=['BBC'],
    provide_context=True,
    dag=dag,
)

def save_dawn_and_bbc_to_csv():
    # Load data from XCom
    dawn_data = '{{ ti.xcom_pull(task_ids="scrape_and_clean_dawn") }}'
    bbc_data = '{{ ti.xcom_pull(task_ids="scrape_and_clean_bbc") }}'
    
    # Merge and save data to CSV
    combined_data = dawn_data + bbc_data
    save_to_csv('combined_data.csv', combined_data)

save_csv_task = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_dawn_and_bbc_to_csv,
    dag=dag,
)

scrape_and_clean_dawn_task >> save_csv_task
scrape_and_clean_bbc_task >> save_csv_task
