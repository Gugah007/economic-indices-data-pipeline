import os
import re
import time
import uuid
import logging
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SchemaField
from google.api_core import exceptions

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
DOWNLOAD_DIR = "/downloads"
CHROME_DRIVER_PATH = "/usr/bin/chromedriver"
CHROME_BINARY_PATH = os.getenv("CHROME_PATH", "/usr/bin/chromium")
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
PROJECT_ID = "ps-eng-dados-ds3x-448302"

def setup_chrome_driver(download_dir):
    """
    Sets up the Chrome driver with specific options.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.binary_location = CHROME_BINARY_PATH

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True
    }
    chrome_options.add_experimental_option("prefs", prefs)

    return webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=chrome_options)

def wait_for_file_download(driver, download_xpath, folder, timeout=60):
    """
    Waits for a new file to be downloaded in the specified folder.
    """
    files_before = set(os.listdir(folder))
    start_time = time.time()

    driver.find_element(By.XPATH, download_xpath).click()

    while time.time() - start_time < timeout:
        files_after = set(os.listdir(folder))
        new_files = files_after - files_before

        for file in new_files:
            if file.endswith(".xlsx"):
                logging.info(f"File downloaded: {file}")
                return os.path.join(folder, file)

        time.sleep(1)
    raise TimeoutError("Timeout waiting for file download.")

def sanitize_column_names(df):
    """
    Sanitizes column names to be compatible with BigQuery.
    """
    df.columns = [re.sub(r"[^a-zA-Z0-9_]+", "_", col).lower() for col in df.columns]
    return df

def upload_to_bigquery(dataframe, table_id, schema):
    """
    Uploads a DataFrame to a BigQuery table.
    """
    try:
        client = bigquery.Client(project=PROJECT_ID)

        job_config = LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND"
        )

        job = client.load_table_from_dataframe(dataframe.astype(str), table_id, job_config=job_config)
        job.result()
        logging.info(f"Data successfully loaded to table {table_id}.")
    except Exception as e:
        logging.error(f"Error uploading data to BigQuery: {e}")
        raise

def process_excel_file(filepath, table_id, sheet_name, columns, num_rows):
    """
    Processes an Excel file, sanitizes the data, and returns a DataFrame.
    """
    try:
        skiprows = 1 if "icc" in table_id.lower() else 0
        df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=num_rows, skiprows=skiprows, header=0)
        df.columns = columns
        df = sanitize_column_names(df)
        df['load_timestamp'] = datetime.utcnow()
        return df
    except Exception as e:
        logging.error(f"Error processing Excel file: {e}")
        raise

def create_bigquery_dataset(dataset_id):
    """
    Creates a BigQuery dataset if it does not exist.
    """
    client = bigquery.Client(project=PROJECT_ID)
    try:
        client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} already exists.")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        logging.info(f"Dataset {dataset_id} created successfully.")

def create_trusted_dataset(dataset_name):
    """
    Creates the trusted dataset if it doesn't exist
    """
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{dataset_name}"
    
    try:
        client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} already exists.")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        logging.info(f"Dataset {dataset_id} created successfully.")

def transform_icc_data(raw_table_id, trusted_dataset):
    """
    Transforms ICC data from raw to trusted layer
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{trusted_dataset}.icc_trusted` AS
    SELECT
        CAST(mes AS DATE) AS mes, 
        CAST(icc AS FLOAT64) AS icc,
        CAST(icc_ate_10_sm AS FLOAT64) AS icc_ate_10_sm,
        CAST(icc_mais_de_10_sm AS FLOAT64) AS icc_mais_de_10_sm,
        IFNULL(IF(IS_NAN(CAST(icc_homens AS FLOAT64)), 0, CAST(icc_homens AS FLOAT64)), 0) AS icc_homens,
        IFNULL(IF(IS_NAN(CAST(icc_mulheres AS FLOAT64)), 0, CAST(icc_mulheres AS FLOAT64)), 0) AS icc_mulheres,
        IFNULL(IF(IS_NAN(CAST(icc_ate_35_anos AS FLOAT64)), 0, CAST(icc_ate_35_anos AS FLOAT64)), 0) AS icc_ate_35_anos,
        IFNULL(IF(IS_NAN(CAST(icc_mais_de_35_anos AS FLOAT64)), 0, CAST(icc_mais_de_35_anos AS FLOAT64)), 0) AS icc_mais_de_35_anos,
        CAST(icea AS FLOAT64) AS icea,
        CAST(icea_ate_10_sm AS FLOAT64) AS icea_ate_10_sm,
        CAST(icea_mais_de_10_sm AS FLOAT64) AS icea_mais_de_10_sm,
        IFNULL(IF(IS_NAN(CAST(icea_homens AS FLOAT64)), 0, CAST(icea_homens AS FLOAT64)), 0) AS icea_homens,
        IFNULL(IF(IS_NAN(CAST(icea_mulheres AS FLOAT64)), 0, CAST(icea_mulheres AS FLOAT64)), 0) AS icea_mulheres,
        IFNULL(IF(IS_NAN(CAST(icea_ate_35_anos AS FLOAT64)), 0, CAST(icea_ate_35_anos AS FLOAT64)), 0) AS icea_ate_35_anos,
        IFNULL(IF(IS_NAN(CAST(icea_mais_de_35_anos AS FLOAT64)), 0, CAST(icea_mais_de_35_anos AS FLOAT64)), 0) AS icea_mais_de_35_anos,
        CAST(iec AS FLOAT64) AS iec,
        CAST(iec_ate_10_sm AS FLOAT64) AS iec_ate_10_sm,
        CAST(iec_mais_de_10_sm AS FLOAT64) AS iec_mais_de_10_sm,
        IFNULL(IF(IS_NAN(CAST(iec_homens AS FLOAT64)), 0, CAST(iec_homens AS FLOAT64)), 0) AS iec_homens,
        IFNULL(IF(IS_NAN(CAST(iec_mulheres AS FLOAT64)), 0, CAST(iec_mulheres AS FLOAT64)), 0) AS iec_mulheres,
        IFNULL(IF(IS_NAN(CAST(iec_ate_35_anos AS FLOAT64)), 0, CAST(iec_ate_35_anos AS FLOAT64)), 0) AS iec_ate_35_anos,
        IFNULL(IF(IS_NAN(CAST(iec_mais_de_35_anos AS FLOAT64)), 0, CAST(iec_mais_de_35_anos AS FLOAT64)), 0) AS iec_mais_de_35_anos,
        CAST(load_timestamp AS TIMESTAMP) as load_timestamp
    FROM `{raw_table_id}`
    """
    
    job = client.query(query)
    job.result()
    logging.info("ICC data transformed successfully")

def transform_icf_data(raw_table_id, trusted_dataset):
    """
    Transforms ICF data from raw to trusted layer
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{trusted_dataset}.icf_trusted` AS
    SELECT
        CAST(mes AS DATE) AS mes,
        CAST(icf AS FLOAT64) AS icf,
        CAST(icf_ate_10_sm AS FLOAT64) AS icf_ate_10_sm,
        CAST(icf_mais_de_10_sm AS FLOAT64) AS icf_mais_de_10_sm,
        CAST(emprego_atual AS FLOAT64) AS emprego_atual,
        CAST(emprego_atual_ate_10_sm AS FLOAT64) AS emprego_atual_ate_10_sm,
        CAST(emprego_atual_mais_de_10_sm AS FLOAT64) AS emprego_atual_mais_de_10_sm,
        CAST(perspectiva_profissional AS FLOAT64) AS perspectiva_profissional,
        CAST(perspectiva_profissional_ate_10_sm AS FLOAT64) AS perspectiva_profissional_ate_10_sm,
        CAST(perspectiva_profissional_mais_de_10_sm AS FLOAT64) AS perspectiva_profissional_mais_de_10_sm,
        CAST(renda_atual AS FLOAT64) AS renda_atual,
        CAST(renda_atual_ate_10_sm AS FLOAT64) AS renda_atual_ate_10_sm,
        CAST(renda_atual_mais_de_10_sm AS FLOAT64) AS renda_atual_mais_de_10_sm,
        CAST(acesso_a_credito AS FLOAT64) AS acesso_a_credito,
        CAST(acesso_a_credito_ate_10_sm AS FLOAT64) AS acesso_a_credito_ate_10_sm,
        CAST(acesso_a_credito_mais_de_10_sm AS FLOAT64) AS acesso_a_credito_mais_de_10_sm,
        CAST(nivel_consumo_atual AS FLOAT64) AS nivel_consumo_atual,
        CAST(nivel_consumo_atual_ate_10_sm AS FLOAT64) AS nivel_consumo_atual_ate_10_sm,
        CAST(nivel_consumo_atual_mais_de_10_sm AS FLOAT64) AS nivel_consumo_atual_mais_de_10_sm,
        CAST(perspectiva_consumo AS FLOAT64) AS perspectiva_consumo,
        CAST(perspectiva_consumo_ate_10_sm AS FLOAT64) AS perspectiva_consumo_ate_10_sm,
        CAST(perspectiva_consumo_mais_de_10_sm AS FLOAT64) AS perspectiva_consumo_mais_de_10_sm,
        CAST(momento_duraveis AS FLOAT64) AS momento_duraveis,
        CAST(momento_duraveis_ate_10_sm AS FLOAT64) AS momento_duraveis_ate_10_sm,
        CAST(momento_duraveis_mais_de_10_sm AS FLOAT64) AS momento_duraveis_mais_de_10_sm,
        CAST(load_timestamp AS TIMESTAMP) as load_timestamp
    FROM `{raw_table_id}`
    """
    
    job = client.query(query)
    job.result()
    logging.info("ICF data transformed successfully")

def transform_refined_data(trusted_dataset, refined_dataset):
    """
    Transforms trusted data to refined layer
    """
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{refined_dataset}.icf_icc_refined` AS
    WITH icc_data AS (
        SELECT
            FORMAT_DATE('%Y-%m', mes) AS ano_mes,
            icc AS icc_indice,
            LAG(icc) OVER(ORDER BY mes) AS prev_icc
        FROM
            `{PROJECT_ID}.{trusted_dataset}.icc_trusted`
    ),

    icf_data AS (
        SELECT
            FORMAT_DATE('%Y-%m', mes) AS ano_mes,
            icf AS icf_indice,
            LAG(icf) OVER(ORDER BY mes) AS prev_icf
        FROM
            `{PROJECT_ID}.{trusted_dataset}.icf_trusted`
    ),

    joined_data AS (
        SELECT
            i.ano_mes,
            i.icc_indice,
            CASE
                WHEN i.prev_icc IS NULL OR i.prev_icc = 0 THEN NULL
                ELSE ( (i.icc_indice - i.prev_icc) / i.prev_icc ) * 100
            END AS icc_variacao,
            
            f.icf_indice,
            CASE
                WHEN f.prev_icf IS NULL OR f.prev_icf = 0 THEN NULL
                ELSE ( (f.icf_indice - f.prev_icf) / f.prev_icf ) * 100
            END AS icf_variacao
        FROM icc_data AS i
        JOIN icf_data AS f
            USING (ano_mes)
    )

    SELECT
        ano_mes,
        icc_indice,
        icc_variacao,
        icf_indice,
        icf_variacao,
        CURRENT_TIMESTAMP() AS load_timestamp
    FROM
        joined_data
    ORDER BY ano_mes ASC
    """
    
    job = client.query(query)
    job.result()
    logging.info("Refined data transformed successfully")

def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    if not GOOGLE_CREDENTIALS:
        raise FileNotFoundError("Google credentials file not found.")

    # Gerar UUID único para todos os datasets
    dataset_uuid = uuid.uuid4().hex[:6]
    
    # Criar dataset raw
    raw_dataset_id = f"{PROJECT_ID}.economic_indices_raw_{dataset_uuid}"
    create_bigquery_dataset(raw_dataset_id)
    
    # Criar dataset trusted
    trusted_dataset = f"economic_indices_trusted_{dataset_uuid}"
    create_trusted_dataset(trusted_dataset)
    
    # Criar dataset refined
    refined_dataset = f"economic_indices_refined_{dataset_uuid}"
    create_trusted_dataset(refined_dataset)  # Podemos usar a mesma função pois a lógica é a mesma

    indices = [
        {
            "url": "https://www.fecomercio.com.br/pesquisas/indice/icc",
            "filename": "icc.xlsx",
            "raw_table": f"{raw_dataset_id}.icc_raw",
            "trusted_table": f"{PROJECT_ID}.{trusted_dataset}.icc_trusted",
            "sheet": "SÉRIE",
            "columns": [
                "mes", "icc", "icc_ate_10_sm", "icc_mais_de_10_sm", "icc_homens", "icc_mulheres",
                "icc_ate_35_anos", "icc_mais_de_35_anos", "icea", "icea_ate_10_sm", "icea_mais_de_10_sm",
                "icea_homens", "icea_mulheres", "icea_ate_35_anos", "icea_mais_de_35_anos", "iec",
                "iec_ate_10_sm", "iec_mais_de_10_sm", "iec_homens", "iec_mulheres", "iec_ate_35_anos",
                "iec_mais_de_35_anos"
            ],
            "rows": 368,
            "transform_function": transform_icc_data
        },
        {
            "url": "https://www.fecomercio.com.br/pesquisas/indice/icf",
            "filename": "icf.xlsx",
            "raw_table": f"{raw_dataset_id}.icf_raw",
            "trusted_table": f"{PROJECT_ID}.{trusted_dataset}.icf_trusted",
            "sheet": "Série Histórica",
            "columns": [
                "mes", "icf", "icf_ate_10_sm", "icf_mais_de_10_sm",
                "emprego_atual", "emprego_atual_ate_10_sm", "emprego_atual_mais_de_10_sm",
                "perspectiva_profissional", "perspectiva_profissional_ate_10_sm", "perspectiva_profissional_mais_de_10_sm",
                "renda_atual", "renda_atual_ate_10_sm", "renda_atual_mais_de_10_sm",
                "acesso_a_credito", "acesso_a_credito_ate_10_sm", "acesso_a_credito_mais_de_10_sm",
                "nivel_consumo_atual", "nivel_consumo_atual_ate_10_sm", "nivel_consumo_atual_mais_de_10_sm",
                "perspectiva_consumo", "perspectiva_consumo_ate_10_sm", "perspectiva_consumo_mais_de_10_sm",
                "momento_duraveis", "momento_duraveis_ate_10_sm", "momento_duraveis_mais_de_10_sm"
            ],
            "rows": 181,
            "transform_function": transform_icf_data
        }
    ]

    driver = setup_chrome_driver(DOWNLOAD_DIR)

    try:
        for index in indices:
            driver.get(index["url"])
            time.sleep(2)
            filepath = wait_for_file_download(driver, '//a[@class="download"]', DOWNLOAD_DIR)
            
            # Process and upload raw data
            df = process_excel_file(filepath, index["raw_table"], index["sheet"], index["columns"], index["rows"])
            schema = [SchemaField(name, "STRING") for name in df.columns]
            upload_to_bigquery(df, index["raw_table"], schema)
            
            # Transform and upload trusted data
            index["transform_function"](index["raw_table"], trusted_dataset)
            
            # Clean up downloaded file
            os.remove(filepath)
        
        # Transform and upload refined data
        transform_refined_data(trusted_dataset, refined_dataset)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()