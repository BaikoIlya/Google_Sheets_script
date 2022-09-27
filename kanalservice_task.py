import gspread
import psycopg2
import requests
import time
import xml.etree.ElementTree as ET
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


RETRY_TIME = 120
pgsql_password = '153794862'
pgsql_port = '5432'
pgsql_database_name = 'postgres_db'
filename = 'clever-cortex-363513-b136106c0d36.json'
table_name = 'Kanalservice'


def get_dollar_value() -> float:
    '''Функция получает актуальный курс Доллара к рублю.'''
    response = requests.get('http://www.cbr.ru/scripts/XML_daily.asp')
    structure = ET.fromstring(response.content)
    dollar = structure.find("./*[@ID='R01235']/Value")
    value = dollar.text.replace(',', '.')
    return float(value)


def get_all_records_from_google_sheet(filename: str, table_name: str) -> list:
    '''Получает все значения из таблицы. Возвращает значения и список номеров заказов.'''
    gc = gspread.service_account(filename=filename)
    sh = gc.open(table_name)
    worksheet = sh.sheet1
    return [worksheet.get_all_records(), worksheet.col_values(2)[1:]]


def change_in_records(last_records: list, actual_records: list) -> list:
    '''Функция возвращает только изменившиеся данные в гугл таблице для дальнейшей обработки.'''
    changes = []
    for _ in actual_records:
        if _ not in last_records:
            changes.append(_)
    return changes


def orders_delete_list(last_orders: list, actual_orders: list) -> list:
    '''Функция генерирует список заказов на удаление, если они были удалены из гугл таблицы.'''
    delete_list = []
    for _ in last_orders:
        if _ not in actual_orders:
            delete_list.append(_)
    return delete_list


def check_or_create_database(
        pgsql_password: str,
        pgsql_port: str,
        pgsql_database_name: str
        ) -> None:
    '''Проверяет существует ли заданная БД, если нет, создаёт её.'''
    try:
        connection = psycopg2.connect(
            user='postgres',
            password=pgsql_password,
            port=pgsql_port
        )
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        all_databases = "SELECT datname FROM pg_database"
        cursor.execute(all_databases)
        record = cursor.fetchall()
        find_db = ([item for item in record if item[0] == pgsql_database_name])
        if not find_db:
            sql_create_database = f'create database {pgsql_database_name}'
            cursor.execute(sql_create_database)
    except (Exception, Error) as error:
        print('Ошибка при создани БД в PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    return


def check_or_create_table(
        pgsql_password: str,
        pgsql_port: str,
        pgsql_database_name: str
        ) -> None:
    '''Создаёт таблицу внутру БД, если такая еще не создана.'''
    try:
        connection = psycopg2.connect(
            user='postgres',
            password=pgsql_password,
            port=pgsql_port,
            database=pgsql_database_name
        )
        cursor = connection.cursor()
        create_table = '''
        CREATE TABLE IF NOT EXISTS delivery_table (
            id serial PRIMARY KEY,
            order_number integer UNIQUE NOT NULL,
            price_dollars  integer NOT NULL,
            price_rubles   real NOT NULL,
            order_date     date NOT NULL
            ); 
        '''
        cursor.execute(create_table)
        connection.commit()
    except (Exception, Error) as error:
        print('Ошибка при создании таблицы в PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()
    return


def work_with_data(
        pgsql_password: str,
        pgsql_port: str,
        pgsql_database_name: str,
        dollar_value: float,
        change: list,
        delete_list: list
        ) -> None:
    '''Функция обрабатывает все полученные и измененные данные из таблицы.'''
    try:
        connection = psycopg2.connect(
            user='postgres',
            password=pgsql_password,
            port=pgsql_port,
            database=pgsql_database_name
        )
        cursor = connection.cursor()
        orders_on_db = 'SELECT order_number FROM delivery_table'
        cursor.execute(orders_on_db)
        orders_tuples = cursor.fetchall()
        orders = [order[0] for order in orders_tuples]
        insert_sql = '''
        INSERT INTO delivery_table (
        order_number, price_dollars, price_rubles, order_date)
        VALUES (%s, %s, %s, %s)
        '''
        update_sql = '''
        UPDATE delivery_table 
        SET price_dollars = %s, price_rubles = %s, order_date = %s 
        WHERE order_number = %s
        '''
        delete_sql = 'DELETE FROM delivery_table WHERE order_number = %s'
        for record in change:
            if record['заказ №'] not in orders:
                cursor.execute(insert_sql, [
                    record['заказ №'],
                    record['стоимость,$'],
                    record['стоимость,$'] * dollar_value,
                    record['срок поставки']
                    ]
                )
            else:
                cursor.execute(update_sql, [
                    record['стоимость,$'],
                    record['стоимость,$'] * dollar_value,
                    record['срок поставки'],
                    record['заказ №']
                    ]
                )
        for order in delete_list:
            cursor.execute(delete_sql, [order, ])
        connection.commit()
    except (Exception, Error) as error:
        print('Ошибка при работе с данными в PostgreSQL', error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def main():
    '''Основная функция, работает постоянно с устанавливаемым временем ожидания мужду запросами.'''
    print('Скрипт запущен.')
    last_records = []
    last_orders = []
    check_or_create_database(
        pgsql_password=pgsql_password,
        pgsql_port=pgsql_port,
        pgsql_database_name=pgsql_database_name
    )
    check_or_create_table(
        pgsql_password=pgsql_password,
        pgsql_port=pgsql_port,
        pgsql_database_name=pgsql_database_name
    )
    while True:
        try:
            actual_records, actual_orders = get_all_records_from_google_sheet(
                filename,
                table_name
            )
            if actual_records != last_records:
                if last_orders:
                    print('Были внесены изменения в Гугл таблицу')
                dollar_value = get_dollar_value()
                change = change_in_records(last_records, actual_records)
                delete_list = orders_delete_list(last_orders, actual_orders)
                work_with_data(
                    pgsql_password=pgsql_password,
                    pgsql_port=pgsql_port,
                    pgsql_database_name=pgsql_database_name,
                    dollar_value=dollar_value,
                    change=change,
                    delete_list=delete_list
                )
                last_records = actual_records
                last_orders = actual_orders
        except Exception as error:
            print(error)
        finally:
            print(f'Данные обновятся через {RETRY_TIME} с.')
            time.sleep(RETRY_TIME)


if __name__ == '__main__':
    main()
