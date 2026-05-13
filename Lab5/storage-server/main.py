import os
from datetime import datetime
from typing import List

from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, DateTime, JSON
from sqlalchemy.orm import declarative_base, sessionmaker


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://admin:admin@localhost:5432/people_counting"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()


class DetectionResult(Base):
    __tablename__ = "detection_results"

    id = Column(Integer, primary_key=True, index=True)
    frame_id = Column(Integer, index=True)
    people_count = Column(Integer)
    boxes = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)

app = FastAPI(title="People Counting Storage Server")


class Box(BaseModel):
    x1: int
    y1: int
    x2: int
    y2: int
    confidence: float


class DetectionRequest(BaseModel):
    frame_id: int
    people_count: int
    boxes: List[Box]


@app.get("/")
def root():
    return {
        "message": "Storage server is running"
    }


@app.post("/results")
def save_result(result: DetectionRequest):
    db = SessionLocal()

    new_result = DetectionResult(
        frame_id=result.frame_id,
        people_count=result.people_count,
        boxes=[box.dict() for box in result.boxes],
    )

    db.add(new_result)
    db.commit()
    db.refresh(new_result)

    result_id = new_result.id

    db.close()

    return {
        "message": "Saved successfully",
        "id": result_id,
        "people_count": result.people_count,
    }


@app.get("/results")
def get_results():
    db = SessionLocal()

    results = (
        db.query(DetectionResult)
        .order_by(DetectionResult.id.desc())
        .limit(20)
        .all()
    )

    db.close()

    return [
        {
            "id": item.id,
            "frame_id": item.frame_id,
            "people_count": item.people_count,
            "boxes": item.boxes,
            "created_at": item.created_at,
        }
        for item in results
    ]