Для використання скрипта вам потрибні КЛЮЧ ДЛЯ BigQuery у форматі JSON та файл config.py в якому будуть зберігатися інформація щодо таблиць та доступів до RTB у які заносимо данні,

Путь до Ключа BigQuery треба підставляти сюди:

# Установите переменную окружения GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/maksym_bilovol/python_scripts/key.json"


config.py повинен мати в собі такі дані: PASSWORD, USERNAME, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID

дані повині бути представлені так:

PASSWORD = 'тут пароль від RTB'
USERNAME = 'email від RTB'
GCP_PROJECT_ID = 'id проекту в bigquery'
BQ_DATASET_ID = 'id датасету в bigquery'
BQ_TABLE_ID = 'id таблиці в bigquery'
