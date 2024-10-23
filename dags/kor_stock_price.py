from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import xml.etree.ElementTree as ET
import csv
import os
import logging

CUR_PATH = os.path.dirname(os.path.realpath(__file__))
API_NAME = 'getStockPriceInfo'
DAG_NAME = 'get_stock_price_info'
COMPANIES = [
    "삼성전자", "SK하이닉스", "LG에너지솔루션", "삼성바이오로직스", 
    "현대차", "삼성전자우", "셀트리온", "KB금융", "기아", "신한지주"
]
API_KEY = Variable.get('DATA_GO_API_KEY')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2001, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(days=1),
}

dag = DAG(
    f'{DAG_NAME}_dag',
    default_args=default_args,
    description='한국 주식시세정보 API',
    schedule_interval='@daily',
    catchup=False
)

def fetch(company_name, date):
    api_key = Variable.get('DATA_GO_API_KEY')
    params = {
            'serviceKey': api_key,
            'basDt': date,
            'itmsNm': company_name
    }
    api_url = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/{API_NAME}"
    response = requests.get(api_url, params=params)
    logging.info(f'date: {date} / company: {company_name}')
    
    if response.status_code == 200:
        try:
            root = ET.fromstring(response.content)
            data = []
            for item in root.findall('items'):
                bas_dt = item.find('basDt').text
                srtn_cd = item.find('srtnCd').text
                itms_nm = item.find('itmsNm').text
                mrkt_ctg = item.find('mrktCtg').text
                clpr = item.find('clpr').text
                vs = item.find('vs').text
                fltRt = item.find('fltRt').text
                mkp = item.find('mkp').text
                hipr = item.find('hipr').text
                lopr = item.find('lopr').text
                trqu = item.find('trqu').text
                tr_prc = item.find('trPrc').text
                lstg_st_cnt = item.find('lstgStCnt').text
                data.append([bas_dt, srtn_cd, itms_nm, mrkt_ctg, clpr, vs, fltRt, mkp, hipr, lopr, trqu, tr_prc, lstg_st_cnt])
            return data
        
        except ET.ParseError as e:
            logging.error(f"XML Parse Error: {e}")
            logging.error(f"Response content: {response.content}")
            return []
    else:
        logging.error(f"API call failed with status code: {response.status_code}")
        logging.error(f"Response content: {response.content}")
        return []

def save_to_csv(data, dir_path, date):
    csv_path = f'{dir_path}/{DAG_NAME}_{date}.csv'
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['bas_dt', 'srtn_cd', 'itms_nm', 'mrkt_ctg', 'clpr', 'vs', 'fltRt', 'mkp', 'hipr', 'lopr', 'trqu', 'tr_prc', 'lstg_st_cnt'])
            writer.writerows(data)
            logging.info(f'{csv_path}가 저장되었습니다.')
    except Exception as e:
        logging.error(f"Error: {e}")

def fetch_and_save(**context):
    execution_date = datetime.strptime(context['ds_nodash'], '%Y%m%d')
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day
    dir_path = os.path.join(CUR_PATH, f'output/transaction_files/{DAG_NAME}/{year}/{month}/{day}')
    
    try:
        os.makedirs(dir_path)
        logging.info(f'{dir_path}가 생성되었습니다.')
    except Exception as e:
        logging.error(f'Error: {e}')
    
    all_data = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for company in COMPANIES:
            futures.append(executor.submit(fetch, company, execution_date))

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)

        save_to_csv(all_data, dir_path, execution_date)

fetch_and_save_task = PythonOperator(
    task_id='fetch_and_save',
    python_callable=fetch_and_save,
    dag=dag
)

fetch_and_save_task
