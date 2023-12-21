import pandas as pd
from database import connect_to_database, create_attendance_table
from decimal import Decimal
import json

def load_dataset(file_path):
    file_extension = file_path.split('.')[-1]
    if file_extension == 'csv':
        return pd.read_csv(file_path)
    elif file_extension in ['xls', 'xlsx']:
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")

def extract_sex_data(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the dataframe.")
    
    return df[column_name]

def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None

import json

def specific_monthevent(cursor):
    cursor.execute("""
        SELECT event_name, MONTH(date_filed) AS event_month, SUM(count) AS attendance_count
        FROM attendance
        GROUP BY event_name, MONTH(date_filed);
    """)

    rows = cursor.fetchall()

    # Custom JSON encoder to handle Decimal objects
    class DecimalEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, Decimal):
                return float(o)
            return super(DecimalEncoder, self).default(o)

    result_list = [{"event_name": row[0], "event_month": row[1], "attendance_count": row[2]} for row in rows]
    result_json = json.dumps(result_list, cls=DecimalEncoder)

    return result_json

        

def insert_data_into_attendance(cursor, sex_counts, event_name, custom_date):
    if not table_exists(cursor, 'attendance'):
        create_attendance_table(cursor)

    for category, count in sex_counts.items():
        cursor.execute("""
            INSERT INTO attendance (category, count, date_filed, event_name) 
            VALUES (%s, %s, %s, %s)
        """, (category, count, custom_date, event_name))

def monthly_attendance(cursor):
    cursor.execute("""
        SELECT MONTH(date_filed) AS month, SUM(count) AS total_count
        FROM attendance
        GROUP BY month;
    """)
    rows = cursor.fetchall()
    result_list = [{"month": month, "total_count": float(total_count)} for month, total_count in rows]
    return result_list

def monthly_distribution(cursor):
    cursor.execute("""
        SELECT 
            DATE_FORMAT(date_filed, '%Y-%m') AS month_category,
            CASE 
                WHEN category = 'Male' THEN 'Male'
                WHEN category = 'Female' THEN 'Female'
                ELSE 'Others'
            END AS grouped_category,
            COUNT(*) AS count_per_category
        FROM 
            attendance
        GROUP BY 
            month_category, grouped_category
        ORDER BY 
            month_category, grouped_category;
    """)

    rows = cursor.fetchall()
    result_list = [{"month": row[0], "category": row[1], "total_count": float(row[2])} for row in rows]
    return result_list

def quarterly_distribution(cursor):
    cursor.execute("""
        SELECT 
            CONCAT(YEAR(date_filed), '-Q', QUARTER(date_filed)) AS quarter_category,
            CASE 
                WHEN category = 'Male' THEN 'Male'
                WHEN category = 'Female' THEN 'Female'
                ELSE 'Others'
            END AS grouped_category,
            COUNT(*) AS count_per_category
        FROM 
            attendance
        GROUP BY 
            quarter_category, grouped_category
        ORDER BY 
            quarter_category, grouped_category;
    """)

    rows = cursor.fetchall()
    result_list = [{"quarter": row[0], "category": row[1], "total_count": float(row[2])} for row in rows]

    return result_list

def main():
    file_path = 'data.xlsx'
    event_name = 'Intramurals'
    column_name = 'Sex'
    custom_date = '2024-1-08 10:00:00'
    df = load_dataset(file_path)
    sex_column = extract_sex_data(df, column_name)
    sex_counts = sex_column.value_counts()
    connection = connect_to_database()

    try:
        with connection.cursor() as cursor:
            insert_data_into_attendance(cursor, sex_counts, event_name, custom_date)
            connection.commit()

            print("Total Attendance Monthly: ", monthly_attendance(cursor))
            print("Gender Distribution Monthly:", monthly_distribution(cursor))
            print("Gender Distribution Quarterly:", quarterly_distribution(cursor))
            print("Specific Month Event:", specific_monthevent(cursor))

    finally:
        connection.close()

if __name__ == "__main__":
    main()
