import pymysql

def connect_to_database():
    return pymysql.connect(
        host='localhost',
        user='root',
        password='',
        database='gadsystemdb',
        # cursorclass=pymysql.cursors.DictCursor
    )

def create_attendance_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genders (
            id INT AUTO_INCREMENT PRIMARY KEY,
            category VARCHAR(255) NOT NULL,
            count INT NOT NULL,
            date_filed DATETIME NOT NULL,
            event_name VARCHAR(255) NOT NULL
        )
    """)