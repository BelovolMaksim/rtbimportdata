from datetime import date, timedelta
from operator import attrgetter
from google.cloud.exceptions import NotFound
import pandas as pd
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\Projects\cred.json"

from rtbhouse_sdk.client import BasicAuth, Client
from rtbhouse_sdk.schema import CountConvention, StatsGroupBy, StatsMetric
from tabulate import tabulate
from google.cloud import bigquery

from config import PASSWORD, USERNAME, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID

def upload_data_to_bigquery(data_frame):
    # Инициализация клиента BigQuery
    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Получение ссылки на таблицу в BigQuery
    dataset_ref = client.dataset(BQ_DATASET_ID)
    table_ref = dataset_ref.table(BQ_TABLE_ID)

    # Проверка существования таблицы
    try:
        table = client.get_table(table_ref)
    except NotFound:
        # Если таблицы нет, создаем её
        schema = [bigquery.SchemaField(column, "STRING") for column in data_frame.columns]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)

    # Загрузка данных в таблицу
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True  # Автоматически определить схему таблицы на основе данных

    with open("data.csv", "w") as data_file:
        data_file.write(data_frame.to_csv(index=False))

    with open("data.csv", "rb") as data_file:
        job = client.load_table_from_file(data_file, table_ref, job_config=job_config)

    job.result()  # Ждем завершения загрузки

if __name__ == "__main__":
    with Client(auth=BasicAuth(USERNAME, PASSWORD)) as api:
        advertisers = api.get_advertisers()
        day_to = date.today()
        day_from = day_to - timedelta(days=30)
        group_by = [StatsGroupBy.DAY]
        metrics = [
            StatsMetric.IMPS_COUNT,
            StatsMetric.CLICKS_COUNT,
            StatsMetric.CAMPAIGN_COST,
            StatsMetric.CONVERSIONS_COUNT,
            StatsMetric.CTR
        ]
        stats = api.get_rtb_stats(
            advertisers[0].hash,
            day_from,
            day_to,
            group_by,
            metrics,
            count_convention=CountConvention.ATTRIBUTED_POST_CLICK,
        )
    columns = group_by + metrics
    data_frame = [
        [getattr(row, c.name.lower()) for c in columns]
        for row in reversed(sorted(stats, key=attrgetter("day")))
    ]
    print(tabulate(data_frame, headers=columns))

    if data_frame:
        # Добавляем доработку для загрузки данных в BigQuery
        df = pd.DataFrame(data_frame, columns=columns)
        upload_data_to_bigquery(df)
    else:
        print("Нет данных для загрузки в BigQuery.")
