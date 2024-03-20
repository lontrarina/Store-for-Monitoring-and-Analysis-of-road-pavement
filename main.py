from typing import List, Set
from sqlalchemy import Table, Column, Integer, String, Float, DateTime, create_engine, MetaData, select
from config import *

from sqlalchemy.orm import sessionmaker, Session
from fastapi import FastAPI, HTTPException, Depends
import json
from starlette.websockets import WebSocket, WebSocketDisconnect
from datetime import datetime
from pydantic import BaseModel, field_validator

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) #?

metadata = MetaData()

# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)


# SQLAlchemy model
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator('timestamp', mode='before')
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).")


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


app = FastAPI()
subscriptions: Set[WebSocket] = set()


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)



# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# FastAPI CRUDL endpoints--------------------------------------------------------------------------------------------------





#Get list of data
@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data(db: Session = Depends(get_db)):
    try:
        processed_agent_data_ = db.query(processed_agent_data).all()
        return processed_agent_data_
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



#Get data by Id
@app.get("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def read_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    try:
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        result = db.execute(query).first()

        if result is None:
            raise HTTPException(status_code=404, detail="Data not found")

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




#Create data
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: ProcessedAgentData, db: Session = Depends(get_db)):
    try:
        user_id = data.agent_data.user_id
        road_state = data.road_state
        agent_data = data.agent_data

        db.execute(
            processed_agent_data.insert().values(
                road_state=road_state,
                user_id=user_id,
                x=agent_data.accelerometer.x,
                y=agent_data.accelerometer.y,
                z=agent_data.accelerometer.z,
                latitude=agent_data.gps.latitude,
                longitude=agent_data.gps.longitude,
                timestamp=agent_data.timestamp
            )
        )
        db.commit()
        data_to_send = {
            "road_state": road_state,
            "agent_data": {
                "user_id": user_id,
                "accelerometer": {
                    "x": agent_data.accelerometer.x,
                    "y": agent_data.accelerometer.y,
                    "z": agent_data.accelerometer.z
                },
                "gps": {
                    "latitude": agent_data.gps.latitude,
                    "longitude": agent_data.gps.longitude
                },
                "timestamp": agent_data.timestamp.isoformat()
            }
        }
        await send_data_to_subscribers(user_id, data_to_send)  # Надіслати дані підписникам
        return {"message": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



#Update data
@app.put("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData, db: Session = Depends(get_db)):
    try:
        # Отримуємо дані для оновлення за допомогою id
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        existing_data = db.execute(query).first()

        # Перевіряємо, чи існує запис з вказаним id
        if existing_data is None:
            raise HTTPException(status_code=404, detail="Data not found")

        # Оновлюємо дані
        agent_data = data.agent_data
        db.execute(
            processed_agent_data.update()
            .where(processed_agent_data.c.id == processed_agent_data_id)
            .values(
                road_state=data.road_state,
                user_id=agent_data.user_id,
                x=agent_data.accelerometer.x,
                y=agent_data.accelerometer.y,
                z=agent_data.accelerometer.z,
                latitude=agent_data.gps.latitude,
                longitude=agent_data.gps.longitude,
                timestamp=agent_data.timestamp
            )
        )
        db.commit()

        # Повертаємо оновлені дані
        updated_data = ProcessedAgentDataInDB(
            id=processed_agent_data_id,
            road_state=data.road_state,
            user_id=agent_data.user_id,
            x=agent_data.accelerometer.x,
            y=agent_data.accelerometer.y,
            z=agent_data.accelerometer.z,
            latitude=agent_data.gps.latitude,
            longitude=agent_data.gps.longitude,
            timestamp=agent_data.timestamp
        )
        return updated_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



#Delete data
@app.delete("/processed_agent_data/{processed_agent_data_id}", response_model=ProcessedAgentDataInDB)
def delete_processed_agent_data(processed_agent_data_id: int, db: Session = Depends(get_db)):
    try:
        # Отримуємо дані, які потрібно видалити за допомогою id
        query = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        existing_data = db.execute(query).first()

        # Перевіряємо, чи існує запис з вказаним id
        if existing_data is None:
            raise HTTPException(status_code=404, detail="Data not found")

        # Видаляємо дані
        db.execute(processed_agent_data.delete().where(processed_agent_data.c.id == processed_agent_data_id))
        db.commit()

        # Повертаємо видалені дані
        return existing_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
