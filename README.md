# Тестовое задания для ООО "Каналсервис"
## Описание:
Скрипт позволяет в онлайн режиме получать данный из Google Sheets и вносить их в базу данных PostgreSQL.
Для работы с данным скриптом предварительно нужно установить PostgreSQL с официального сайта https://www.postgresql.org/ .
Гугл таблица: https://docs.google.com/spreadsheets/d/1FBvi6jEZ3-AnfTumswPstoVp5eEMUwzsRJmYQPMYJZk/edit#gid=0

### Установка:
Клонировать репозиторий и перейти в него в командной строке:
```
git clone git@github.com:BaikoIlya/kanalservice_test.git
```
```
cd kanalservice_test
```
Cоздать и активировать виртуальное окружение:

```
python3 -m venv env
```

```
source env/bin/activate
```

Установить зависимости из файла requirements.txt:

```
python3 -m pip install --upgrade pip
```

```
pip install -r requirements.txt
```

Открыть в редакторе **_kanalservice_task.py_** и прописать собственные данные:

```
RETRY_TIME = 120 # Время между запросами к гугл таблице в секундах
pgsql_password = '153794862' # Пароль, который вы указывали при установке PostgreSQL
pgsql_port = '5432' # Порт, который вы указывали при установке PostgreSQL 
pgsql_database_name = 'postgres_db' # Имя базы данных в которой вы хотите работать.
```

Сохранить изменения и с активированным виртуальным окружением выполнить команду:

```python kanalservice_task.py```
