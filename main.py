from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
import boto3
import json
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./test.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

metadata = MetaData()
outbox_table = Table("outbox", metadata,
                     Column("id", Integer, primary_key=True, autoincrement=True),
                     Column("message", String),
                     Column("processed", Integer, default=0))

metadata.create_all(bind=engine)

sqs = boto3.client('sqs', region_name=os.getenv("AWS_REGION", "us-east-1"))
queue_url = os.getenv("QUEUE_URL", "https://sqs.your-region.amazonaws.com/your-account-id/your-queue-name")

@app.post("/send/")
def send_message(message: str):
    db = SessionLocal()
    try:
        # Add message to outbox
        ins = outbox_table.insert().values(message=message)
        db.execute(ins)
        db.commit()
        return {"status": "Message added to outbox"}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

def publish_messages():
    db = SessionLocal()
    try:
        with db.begin():
            messages = db.execute(outbox_table.select().where(outbox_table.c.processed == 0)).fetchall()
            for message in messages:
                response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message['message']))
                update = outbox_table.update().where(outbox_table.c.id == message['id']).values(processed=1)
                db.execute(update)
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
