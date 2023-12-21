from fastapi import FastAPI, HTTPException, Form, APIRouter
from pydantic import BaseModel
from fastapi.responses import JSONResponse

from database import connect_to_database

app = APIRouter()

# Assuming you already have a connection to the database
connection = connect_to_database()

class Event(BaseModel):
    event_name: str

# Create event
@app.post("/create/")
async def create_event(event_name: str = Form(...)):
    try:
        with connection.cursor() as cursor:
            sql = "INSERT INTO events (event_name) VALUES (%s)"
            cursor.execute(sql, (event_name,))
            connection.commit()
        return {"message": "Event created successfully", "event_name": event_name}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        

@app.get("/read/")
async def view_event():
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM `events`")
            rows = cursor.fetchall()
            result_list = [{"event_id": row[0], "event_name": row[1]} for row in rows]
        return JSONResponse(content=result_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/list/")
def get_specific_month_event():
    connection = connect_to_database()
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
               SELECT
                    DATE(date_filed) AS date_only,
                    SUM(count) AS total_count,
                    event_name
                FROM
                    attendance
                GROUP BY
                    DATE(date_filed), event_name;

            """)

            rows = cursor.fetchall()
            result_list = []

            for row in rows:
                event_date = row[0].strftime('%Y-%m-%d') if row[0] is not None else None
                result_list.append({"event_date": event_date, "event_count": float(row[1]), "event_name": row[2]})

            return JSONResponse(content=result_list)
    finally:
        connection.close()

# Update event
@app.post("/update/{event_id}")
async def update_event(event_id: int, event_name: str = Form(...)):
    try:
        with connection.cursor() as cursor:
            sql = "UPDATE events SET event_name = %s WHERE id = %s"
            cursor.execute(sql, (event_name, event_id))
            connection.commit()
            if cursor.rowcount > 0:
                return {"message": "Event updated successfully"}
            else:
                raise HTTPException(status_code=404, detail="Event not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Delete event
@app.delete("/delete/{event_id}")
async def delete_event(event_id: int):
    try:
        with connection.cursor() as cursor:
            sql = "DELETE FROM events WHERE id = %s"
            cursor.execute(sql, (event_id,))
            connection.commit()
            if cursor.rowcount > 0:
                return {"message": "Event deleted successfully"}
            else:
                raise HTTPException(status_code=404, detail="Event not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
