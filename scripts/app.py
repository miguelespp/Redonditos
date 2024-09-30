from database import Database
import faker

try:
    
    connection = Database()
    print(connection.get_version())
    
    fake = faker.Faker()

    connection.add_users(fake, 10)
    connection.add_authors(fake, 10)

    connection.commit()
    connection.close()


except (Exception) as error:
    print(error)
finally:print('Connection closed')
