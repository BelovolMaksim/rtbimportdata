Этот скрипт предназначен для загрузки статистических данных из RTBhouse в BigQuery.

#Требования:

Python 3
Установленные библиотеки
Учетные данные RTBhouse (USERNAME, PASSWORD)
Учетные данные Google Cloud Platform и наличие созданного проекта с активированным API BigQuery


#Использование:

Замените значения переменных USERNAME, PASSWORD, GCP_PROJECT_ID, BQ_DATASET_ID, и BQ_TABLE_ID в config.py на свои.
Запустите скрипт с помощью команды python3 your_script.py в терминале.

#Скрипт выполняет следующие шаги:

Запрашивает статистические данные от RTBhouse для указанных кампаний и метрик.
Создает и обрабатывает таблицы данных с использованием библиотеки pandas.
Подключается к BigQuery с использованием учетных данных GCP.
Загружает обработанные данные в указанный набор данных и таблицу в BigQuery.
Примечание: Перед использованием убедитесь, что у вас есть необходимые права доступа и правильные учетные данные.


##Путь до Ключа BigQuery треба підставляти сюди:

 ##Установите переменную окружения GOOGLE_APPLICATION_CREDENTIALS
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/maksym_bilovol/python_scripts/key.json"


##config.py повинен мати в собі такі дані: PASSWORD, USERNAME, GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID

##дані повині бути представлені так:

PASSWORD = 'тут пароль від RTB'
USERNAME = 'email від RTB'
GCP_PROJECT_ID = 'id проекту в bigquery'
BQ_DATASET_ID = 'id датасету в bigquery'
BQ_TABLE_ID = 'id таблиці в bigquery'
