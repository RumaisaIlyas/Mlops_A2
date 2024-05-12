from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scrape_utils import scrape_webpage, clean_text, save_to_csv

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'web_scraping_dag',
    default_args=default_args,
    description='A DAG to automate web scraping, data transformation, and storage',
    schedule_interval='@daily',  # Run daily
)

# Define the function to scrape webpage
def scrape_and_transform(url, **kwargs):
    dawn_links, dawn_titles, dawn_descriptions = scrape_webpage(url)
    cleaned_dawn_titles = [clean_text(title) for title in dawn_titles]
    cleaned_dawn_descriptions = [clean_text(description) for description in dawn_descriptions]
    return cleaned_dawn_titles, cleaned_dawn_descriptions

# Define the function to save data to CSV
def save_to_csv_task(filename, data, **kwargs):
    save_to_csv(filename, data)

# Define the tasks
scrape_dawn_task = PythonOperator(
    task_id='scrape_dawn',
    python_callable=scrape_and_transform,
    op_kwargs={'url': 'https://www.dawn.com/'},
    dag=dag,
)

save_dawn_to_csv_task = PythonOperator(
    task_id='save_dawn_to_csv',
    python_callable=save_to_csv_task,
    op_kwargs={'filename': '/path/to/save/dawn_data.csv'},
    provide_context=True,
    dag=dag,
)

scrape_and_save_bbc_task = PythonOperator(
    task_id='scrape_and_save_bbc',
    python_callable=scrape_and_save_bbc,
    op_kwargs={'url': 'https://www.bbc.com/'},
    provide_context=True,
    dag=dag,
)

save_bbc_to_csv_task = PythonOperator(
    task_id='save_bbc_to_csv',
    python_callable=save_to_csv_task,
    op_kwargs={'filename': '/path/to/save/bbc_data.csv'},
    provide_context=True,
    dag=dag,
)

# Define task dependencies
scrape_dawn_task >> save_dawn_to_csv_task
scrape_and_save_bbc_task >> save_bbc_to_csv_task
