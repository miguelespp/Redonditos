import os
import psycopg2
import faker
import random
from datetime import date


class Database:

    def __init__(self):
        host = os.getenv('POSTGRES_HOST')
        database = os.getenv('POSTGRES_DB')
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')
        port = os.getenv('POSTGRES_PORT')

        self.conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port= port
        )

    def get_version(self):
        cur = self.conn.cursor()
        cur.execute('SELECT version();')
        db_version = cur.fetchone()
        return db_version

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add_users (self, faker: faker.Faker, num_users: int):
        cur = self.conn.cursor()
        nGender = cur.execute('SELECT COUNT(*) FROM "Gender"')
        if nGender == 0:
            return
        
        for _ in range(num_users):
            temp = {
                'username': faker.name(),
                'email': faker.email(),
                'password_hash': faker.sha256(),
                'password_salt': faker.password(),
                'firstname': faker.first_name(),
                'surname': faker.last_name(),
                'birth_date': faker.date_of_birth(),
                'address': faker.address(),
                'mobile_phone': faker.phone_number(),
                # 1-Male , 2 - Female, 3 - Other, 4- Non Binry
                'gender_id': faker.random_int(1, nGender),
                # 1 - admin, 2 - user
                'role_id': faker.random_int(1, 2)
            }
            query = 'INSERT INTO "User" (username, email, password_hash, password_salt, firstname, surname, birth_date, address, mobile_phone, gender_id, role_id) VALUES (%(username)s, %(email)s, %(password_hash)s, %(password_salt)s, %(firstname)s, %(surname)s, %(birth_date)s, %(address)s, %(mobile_phone)s, %(gender_id)s, %(role_id)s)'
            cur.execute(query, temp)
        cur.close()

    def add_authors(self, faker: faker.Faker, num_authors: int):
        cur = self.conn.cursor()
        nGender = cur.execute('SELECT COUNT(*) FROM "Gender"')
        nCountry = cur.execute('SELECT COUNT(*) FROM "Country"')

        if nGender == 0 or nCountry == 0:
            return
        
        for _ in range(num_authors):
            birth_date = faker.date_of_birth(minimum_age=30)
            death_date = birth_date.replace(year=birth_date.year + random.randint(30, 90))
            if death_date > date.today():
                death_date = None

            temp = {
                'name': faker.name(),
                'birth_date': birth_date,
                'death_date': death_date,
                'bio': faker.text(),
                'gender_id': faker.random_int(1, nGender),
                'country_id': faker.random_int(1, nCountry)
            }

            query = 'INSERT INTO "Author" (name, birth_date, death_date, bio, gender_id, country_id) VALUES (%(name)s, %(birth_date)s, %(death_date)s, %(bio)s, %(gender_id)s, %(country_id)s)'
            cur.execute(query, temp)
        cur.close()

    def add_documents(self, faker: faker.Faker, num_documents: int):
        cur = self.conn.cursor()

        nLanguages = cur.execute('SELECT COUNT(*) FROM "Language"')
        nPublishers = cur.execute('SELECT COUNT(*) FROM "Publisher"')
        nDocumentFormats = cur.execute('SELECT COUNT(*) FROM "DocumentFormat"')
        if nLanguages == 0 or nPublishers == 0 or nDocumentFormats == 0:
            return
        for _ in range(num_documents):
            temp = {
                'title': faker.sentence(),
                'isbn': faker.isbn13(),
                'description': faker.text(),
                'cover_url': faker.image_url(),
                'publication_date': faker.date_this_century(),
                'acquisition_date': faker.date_this_century(),
                'edition': faker.random_int(1, 10),
                'total_pages': faker.random_int(50, 500),
                'external_lend_allowed': faker.boolean(),
                'base_price': faker.random_int(10, 100),
                'total_copies': faker.random_int(1, 10),
                'available_copies': faker.random_int(1, 10),
                'avg_rating': faker.random_int(1, 5),
                'language_id': faker.random_int(1, nLanguages),
                'publisher_id': faker.random_int(1, nPublishers),
                'document_format_id': faker.random_int(1, nDocumentFormats),
                'author_id': faker.random_int(1, 10)
            }
            query = '''
            INSERT INTO "Document" (title, isbn, description, cover_url, publication_date, acquisition_date, edition, total_pages, external_lend_allowed, base_price, total_copies, available_copies, avg_rating, language_id, publisher_id, document_format_id, author_id)
            VALUES (%(title)s, %(isbn)s, %(description)s, %(cover_url)s, %(publication_date)s, %(acquisition_date)s, %(edition)s, %(total_pages)s, %(external_lend_allowed)s, %(base_price)s, %(total_copies)s, %(available_copies)s, %(avg_rating)s, %(language_id)s, %(publisher_id)s, %(document_format_id)s, %(author_id)s)'''
            cur.execute(query, temp)
        cur.close()
