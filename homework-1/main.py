import os
import csv
import psycopg2


"""Скрипт для заполнения данными таблиц в БД Postgres."""

employees = os.path.abspath('north_data/employees_data.csv')
orders = os.path.abspath('north_data/orders_data.csv')
customers = os.path.abspath('north_data/customers_data.csv')

con = psycopg2.connect(host="localhost", database="north", user="postgres", password="Qwerty123")


def employees_add():
    with con:
        with con.cursor() as cursor:
            with open(employees) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute("INSERT INTO employees (employee_id, first_name, last_name, title, birth_date, "
                                   "notes) VALUES (%s, %s, %s, %s, %s, %s)", row)


def customers_add():
    with con:
        with con.cursor() as cursor:
            with open(customers) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute("INSERT INTO customers (customer_id, company_name, contact_name) "
                                   "VALUES (%s, %s, %s)", row)


def orders_add():
    with con:
        with con.cursor() as cursor:
            with open(orders) as file:
                reader = csv.reader(file)
                next(reader)
                for row in reader:
                    cursor.execute("INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_city) "
                                   "VALUES (%s, %s, %s, %s, %s)", row)


if __name__ == "__main__":
    employees_add()
    customers_add()
    orders_add()
    con.close()
