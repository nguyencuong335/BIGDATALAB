import os
import time
import base64
import json

import cv2
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VIDEO_PATH = os.getenv("VIDEO_PATH", "/app/data/video.mp4")
TOPIC_NAME = "camera_frames"


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )

            print("[INFO] Camera server connected to Kafka")
            return producer

        except Exception as error:
            print("[WARN] Kafka is not ready. Retrying...")
            print(error)
            time.sleep(5)


def main():
    producer = create_producer()

    cap = cv2.VideoCapture(VIDEO_PATH)

    if not cap.isOpened():
        print(f"[ERROR] Cannot open video file: {VIDEO_PATH}")
        print("[HINT] Please put your video at: Lab5/data/video.mp4")
        return

    frame_id = 0

    while True:
        success, frame = cap.read()

        if not success:
            print("[INFO] Video ended. Restarting video...")
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            continue

        frame_id += 1

        frame = cv2.resize(frame, (640, 360))

        encode_success, buffer = cv2.imencode(".jpg", frame)

        if not encode_success:
            print(f"[WARN] Cannot encode frame {frame_id}")
            continue

        frame_base64 = base64.b64encode(buffer).decode("utf-8")

        message = {
            "frame_id": frame_id,
            "frame": frame_base64,
        }

        producer.send(TOPIC_NAME, message)
        producer.flush()

        print(f"[CAMERA SERVER] Sent frame {frame_id}")

        time.sleep(0.2)


if __name__ == "__main__":
    main()