import httplib2 
import googleapiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials	
import json
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
import requests as r


@task(task_id='send_telegram_message', retries=3)
def send_telegram_message(chat_id, data_length):
    """Скачать данные из аутха"""

    token = str(Variable.get("TGBOT_TOKEN"))
    r.post(
        f'https://api.telegram.org/bot{token}/sendMessage',
        json={
            "chat_id": chat_id,
            "text": "Записано {} строк в таблицу".format(data_length),
        }
    )

@task(task_id='fetch_users', outlets=Dataset("STG_UNION_MEMBER.union_member"))
def fetch_auth_users():
    CREDENTIALS = json(Variable.get("GOOGLE_OAUTH_CREDENTIALS"))
    AUTH_API_URL = Connection.get_connection_from_secrets('auth_dwh').get_uri().replace("postgres://", "postgresql://")

    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
    service = googleapiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API 
    spreadsheetId = "1VHYnKIEbvC4dPASx8dYOMqSoU_QAqb_asYb3f_Iy5d0" if not str(Variable.get_variable_from_secrets("SPREADSHEET_ID")) else str(Variable.get_variable_from_secrets("SPREADSHEET_ID"))
    driveService = googleapiclient.discovery.build('drive', 'v3', http = httpAuth) # Выбираем работу с Google Drive и 3 версию API

    engine = sa.create_engine(AUTH_API_URL)
    with engine.connect() as conn:
        conn.execute(sa.text("SELECT * FROM STG_UNION_MEMBER.union_member"))
    # access = driveService.permissions().create(
    #     fileId = spreadsheetId,
    #     body = {'type': 'user', 'role': 'writer', 'emailAddress': 'roslavtcev.sv22@physics.msu.ru'},  # Открываем доступ на редактирование
    #     fields = 'id'
    # ).execute()
    results = service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, body = {
        "valueInputOption": "USER_ENTERED", # Данные воспринимаются, как вводимые пользователем (считается значение формул)
        "data": [
            {"range": "Лист номер один!B2:D5",
            "majorDimension": "ROWS",     # Сначала заполнять строки, затем столбцы
            "values": [
                        ["Ячейка B2", "Ячейка C2", "Ячейка D2"], # Заполняем первую строку
                        ['25', "=6*6", "=sin(3,14/2)"]  # Заполняем вторую строку
                    ]}
        ]
    }).execute()
    # print('https://docs.google.com/spreadsheets/d/' + spreadsheetId)