from fastapi import FastAPI, HTTPException,UploadFile, File, Form
from fastapi.responses import JSONResponse
from database import connect_to_database, create_attendance_table
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from decimal import Decimal
import json
from pydantic import BaseModel
import uvicorn
import os
import shutil
import openpyxl
from fastapi.encoders import jsonable_encoder
from datetime import datetime
from eventCrud import app as event_app
from getChart import chart_app

app = FastAPI()




connection = connect_to_database()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # You can replace "*" with specific origins if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(event_app, prefix="/events", tags=["events"])
# app.include_router(attendance_app, prefix="/attendance", tags=["attendance"])
app.include_router(chart_app, prefix="/charts", tags=["charts"])


def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return cursor.fetchone() is not None

def extract_sex_data(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in the dataframe.")

# @app.get("/attendance/monthly")
# def get_monthly_attendance():
#     connection = connect_to_database()

#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT YEAR(date_filed) AS year, MONTH(date_filed) AS month, SUM(count) AS total_count
#                 FROM attendance
#                 GROUP BY year, month;
#             """)
#             rows = cursor.fetchall()

#             # Convert Decimal objects to float for JSON serialization
#             result_list = [{"year": int(year), "month": int(month), "total_count": float(total_count)} for year, month, total_count in rows]

#             return JSONResponse(content=result_list)
#     finally:
#         connection.close()

# @app.get("/attendance/monthly_distribution")
# def get_monthly_distribution():
#     connection = connect_to_database()
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT 
#                     YEAR(date_filed) AS year,
#                     MONTH(date_filed) AS month,
#                     CASE 
#                         WHEN category = 'Male' THEN 'Male'
#                         WHEN category = 'Female' THEN 'Female'
#                         ELSE 'Others'
#                     END AS grouped_category,
#                     SUM(count) AS total_count_per_category
#                 FROM 
#                     attendance
#                 GROUP BY 
#                     year, month, grouped_category
#                 ORDER BY 
#                     year, month, grouped_category;
#             """)
#             rows = cursor.fetchall()

#             result_list = [{"year": row[0], "month": row[1], "category": row[2], "total_count": row[3]} for row in rows]

#             return result_list
#     finally:
#         connection.close()


# @app.get("/attendance/quarterly_distribution")
# def get_quarterly_distribution():
#     connection = connect_to_database()
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT 
#                     YEAR(date_filed) AS year,
#                     QUARTER(date_filed) AS quarter,
#                     CONCAT(YEAR(date_filed), '-Q', QUARTER(date_filed)) AS quarter_category,
#                     CASE 
#                         WHEN category = 'Male' THEN 'Male'
#                         WHEN category = 'Female' THEN 'Female'
#                         ELSE 'Others'
#                     END AS grouped_category,
#                     SUM(count) AS count_per_category
#                 FROM 
#                     attendance
#                 GROUP BY 
#                     year, quarter, quarter_category, grouped_category
#                 ORDER BY 
#                     year, quarter, grouped_category;

#             """)
#             rows = cursor.fetchall()
#             result_list = [{"year": row[0], "quarter": row[1], "category": row[3], "total_count": float(row[4])} for row in rows]
#             return JSONResponse(content=result_list)
#     finally:
#         connection.close()



# @app.get("/attendance/specific_month_event")
# def get_specific_month_event():
#     connection = connect_to_database()
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                 SELECT 
#                     event_name, 
#                     YEAR(date_filed) AS event_year, 
#                     MONTH(date_filed) AS event_month, 
#                     SUM(count) AS attendance_count
#                 FROM 
#                     attendance
#                 GROUP BY 
#                     event_name, event_year, event_month;
#             """)

#             rows = cursor.fetchall()
#             result_list = [{"event_name": row[0], "event_year": row[1], "event_month": row[2], "total_count": float(row[3])} for row in rows]
#             return JSONResponse(content=result_list)
#     finally:
#         connection.close()

# @app.get("/eventList/")
# def get_specific_month_event():
#     connection = connect_to_database()
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("""
#                SELECT
#                     DATE(date_filed) AS date_only,
#                     SUM(count) AS total_count,
#                     event_name
#                 FROM
#                     attendance
#                 GROUP BY
#                     DATE(date_filed), event_name;

#             """)

#             rows = cursor.fetchall()
#             result_list = []

#             for row in rows:
#                 event_date = row[0].strftime('%Y-%m-%d') if row[0] is not None else None
#                 result_list.append({"event_date": event_date, "event_count": float(row[1]), "event_name": row[2]})

#             return JSONResponse(content=result_list)
#     finally:
#         connection.close()

# @app.get("/view_events/")
# async def view_event():
#     try:
#         with connection.cursor() as cursor:
#             cursor.execute("SELECT * FROM `events`")
#             rows = cursor.fetchall()
#             result_list = [{"event_id": row[0], "event_name": row[1]} for row in rows]
#         return JSONResponse(content=result_list)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/events/")
# async def create_event(event_name: str = Form(...)):
#     try:
#         # Execute the SQL query to insert into the 'events' table
#         with connection.cursor() as cursor:
#             sql = "INSERT INTO events (event_name) VALUES (%s)"
#             cursor.execute(sql, (event_name,))
#             connection.commit()

#         return {"message": "Event created successfully", "event_name": event_name}

#     except Exception as e:
#         # If an error occurs, raise an HTTPException
#         raise HTTPException(status_code=500, detail=str(e))
    

def load_dataset(file_path):
    file_extension = file_path.split('.')[-1]
    if file_extension == 'csv':
        return pd.read_csv(file_path)
    elif file_extension in ['xls', 'xlsx']:
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")

def read_xlsx(file: UploadFile):
    try:
        workbook = openpyxl.load_workbook(file.file)
        sheet = workbook.active
        sex_column_data = []
        header_row = list(sheet.iter_rows(min_row=1, max_row=1, values_only=True))[0]
        sex_column_index = None

        for i, value in enumerate(header_row):
            if value == "Sex":
                sex_column_index = i + 1
                break
        if sex_column_index is not None:
            for row in sheet.iter_rows(min_row=2, max_row=sheet.max_row, values_only=True):
                sex_column_data.append(row[sex_column_index - 1])
            df = pd.DataFrame({"Sex": sex_column_data})
            sex_counts = df["Sex"].value_counts().to_dict()

            return sex_counts
        else:
            raise HTTPException(status_code=400, detail="Column 'Sex' not found in the XLSX file.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading XLSX file: {str(e)}")


@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile = File(...), title: str = Form(...), date: str = Form(...)):
    if file.filename.endswith(".xlsx"):
        column_data = read_xlsx(file)
        connection = connect_to_database()
        for category, count in column_data.items():
            insert_attendance(category, count, date, title, connection)
        connection.close()

        response_data = {
            "file": jsonable_encoder(column_data),
            "title": title,
            "date": date
        }
        return JSONResponse(content=response_data, media_type="application/json")
    else:
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an XLSX file.")
    
def insert_attendance(category, count, date_filed, event_name, connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO attendance (category, count, date_filed, event_name)
                VALUES (%s, %s, %s, %s)
            """, (category, count, date_filed, event_name))  
        connection.commit()  # Commit changes to the database
        # print("Insert successful")
    except Exception as e:
        connection.rollback()  # Rollback changes in case of an error
        print(f"Error inserting data into MySQL: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)