from fastapi import APIRouter
from fastapi.responses import JSONResponse
from database import connect_to_database

chart_app = APIRouter()

@chart_app.get("/monthly")
def get_monthly_attendance():
    connection = connect_to_database()

    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT YEAR(date_filed) AS year, MONTH(date_filed) AS month, SUM(count) AS total_count
                FROM attendance
                GROUP BY year, month;
            """)
            rows = cursor.fetchall()

            # Convert Decimal objects to float for JSON serialization
            result_list = [{"year": int(year), "month": int(month), "total_count": float(total_count)} for year, month, total_count in rows]

            return JSONResponse(content=result_list)
    finally:
        connection.close()

@chart_app.get("/monthly_distribution")
def get_monthly_distribution():
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    YEAR(date_filed) AS year,
                    MONTH(date_filed) AS month,
                    CASE 
                        WHEN category = 'Male' THEN 'Male'
                        WHEN category = 'Female' THEN 'Female'
                        ELSE 'Others'
                    END AS grouped_category,
                    SUM(count) AS total_count_per_category
                FROM 
                    attendance
                GROUP BY 
                    year, month, grouped_category
                ORDER BY 
                    year, month, grouped_category;
            """)
            rows = cursor.fetchall()

            result_list = [{"year": row[0], "month": row[1], "category": row[2], "total_count": row[3]} for row in rows]

            return result_list
    finally:
        connection.close()


@chart_app.get("/quarterly_distribution")
def get_quarterly_distribution():
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    YEAR(date_filed) AS year,
                    QUARTER(date_filed) AS quarter,
                    CONCAT(YEAR(date_filed), '-Q', QUARTER(date_filed)) AS quarter_category,
                    CASE 
                        WHEN category = 'Male' THEN 'Male'
                        WHEN category = 'Female' THEN 'Female'
                        ELSE 'Others'
                    END AS grouped_category,
                    SUM(count) AS count_per_category
                FROM 
                    attendance
                GROUP BY 
                    year, quarter, quarter_category, grouped_category
                ORDER BY 
                    year, quarter, grouped_category;

            """)
            rows = cursor.fetchall()
            result_list = [{"year": row[0], "quarter": row[1], "category": row[3], "total_count": float(row[4])} for row in rows]
            return JSONResponse(content=result_list)
    finally:
        connection.close()



@chart_app.get("/specific_month_event")
def get_specific_month_event():
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    event_name, 
                    YEAR(date_filed) AS event_year, 
                    MONTH(date_filed) AS event_month, 
                    SUM(count) AS attendance_count
                FROM 
                    attendance
                GROUP BY 
                    event_name, event_year, event_month;
            """)

            rows = cursor.fetchall()
            result_list = [{"event_name": row[0], "event_year": row[1], "event_month": row[2], "total_count": float(row[3])} for row in rows]
            return JSONResponse(content=result_list)
    finally:
        connection.close()




@chart_app.get("/yearly_distribution")
def get_yearly_distribution():
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    YEAR(date_filed) AS year,
                    CASE 
                        WHEN category = 'Male' THEN 'Male'
                        WHEN category = 'Female' THEN 'Female'
                        ELSE 'Others'
                    END AS grouped_category,
                    SUM(count) AS total_count_per_category
                FROM 
                    attendance
                GROUP BY 
                    year, grouped_category
                ORDER BY 
                    year, grouped_category;
            """)
            rows = cursor.fetchall()

            result_list = [{"year": row[0], "category": row[1], "total_count": float(row[2])} for row in rows]

            return JSONResponse(content=result_list)
    finally:
        connection.close()