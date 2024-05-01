from pymongo import MongoClient
import json
from clickhouse_driver import Client

client = MongoClient('mongodb://localhost:27017')

db = client['books-database']

books = db.books

with open('books.json') as file:
    file_data = json.load(file)

books.insert_many(file_data)

# 1. Вывести список всех книг

for book in books.find():
    print(book)

# 2. Вывести список книг, цена которых больше 30

for book in books.find({'price': {'$gt': 30}}):
    print(book)

# Внести измения в названия книг

def update_book_title(title, new_title):
    books.update_one({'title': title}, {'$set': {'title': new_title}})

update_book_title('A Light in the Attic', 'A Light in the Attic (updated)')

# Удалить книгу по названию

def delete_book(title):
    books.delete_one({'title': title})

delete_book('A Light in the Attic (updated)')

# Скрипт для создания таблицы в ClickHouse и вставки данных из json

client = Client('localhost')

client.execute("CREATE DATABASE IF NOT EXISTS books")

client.execute('CREATE TABLE IF NOT EXISTS books.books (id UInt64, title String, price String, availability String) ENGINE = MergeTree() ORDER BY id ')

print('Table created')
# Втавить данные из json в таблицу
id = 0
for book in file_data:
    id += 1
    client.execute(f"INSERT INTO books.books VALUES ({id}, '{book['title']}', '{book['price']}', {book['availability']})")









