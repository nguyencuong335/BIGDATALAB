import os
import time
import json
import base64

import cv2
import numpy as np
import requests
from kafka import KafkaConsumer
from ultralytics import YOLO


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STORAGE_SERVER_URL = os.getenv("STORAGE_SERVER_URL", "http://localhost:8000")

TOPIC_NAME = "camera_frames"
OUTPUT_DIR = "/app/output"


def create_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda message: json.loads(message.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="people-counting-processing-group",
            )

            print("[INFO] Processing server connected to Kafka")
            return consumer

        except Exception as error:
            print("[WARN] Kafka is not ready. Retrying...")
            print(error)
            time.sleep(5)


def decode_frame(frame_base64):
    frame_bytes = base64.b64decode(frame_base64)
    np_array = np.frombuffer(frame_bytes, np.uint8)
    frame = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

    return frame


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("[INFO] Loading YOLOv8 model...")
    model = YOLO("yolov8n.pt")
    print("[INFO] YOLOv8 model loaded")

    consumer = create_consumer()

    for message in consumer:
        data = message.value

        frame_id = data["frame_id"]
        frame = decode_frame(data["frame"])

        if frame is None:
            print(f"[WARN] Cannot decode frame {frame_id}")
            continue

        results = model(frame, verbose=False)

        boxes = []

        for result in results:
            for box in result.boxes:
                class_id = int(box.cls[0])
                confidence = float(box.conf[0])

                # COCO dataset: class 0 is person
                if class_id == 0 and confidence >= 0.4:
                    xyxy = box.xyxy[0].cpu().numpy()

                    x1 = int(float(xyxy[0]))
                    y1 = int(float(xyxy[1]))
                    x2 = int(float(xyxy[2]))
                    y2 = int(float(xyxy[3]))

                    boxes.append(
                        {
                            "x1": x1,
                            "y1": y1,
                            "x2": x2,
                            "y2": y2,
                            "confidence": round(confidence, 4),
                        }
                    )

                    # Draw bounding box
                    cv2.rectangle(
                        frame,
                        (x1, y1),
                        (x2, y2),
                        (0, 255, 0),
                        2,
                    )

                    label = f"person {confidence:.2f}"

                    cv2.putText(
                        frame,
                        label,
                        (x1, max(y1 - 10, 20)),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.6,
                        (0, 255, 0),
                        2,
                    )

        people_count = len(boxes)

        # Draw people count on image
        cv2.putText(
            frame,
            f"People count: {people_count}",
            (20, 40),
            cv2.FONT_HERSHEY_SIMPLEX,
            1,
            (0, 0, 255),
            2,
        )

        # Save one annotated image every 30 frames
        if frame_id % 30 == 0:
            output_path = f"{OUTPUT_DIR}/annotated_frame_{frame_id}.jpg"
            cv2.imwrite(output_path, frame)
            print(f"[PROCESSING SERVER] Saved annotated image: {output_path}")

        payload = {
            "frame_id": frame_id,
            "people_count": people_count,
            "boxes": boxes,
        }

        try:
            response = requests.post(
                f"{STORAGE_SERVER_URL}/results",
                json=payload,
                timeout=5,
            )

            print(
                f"[PROCESSING SERVER] Frame {frame_id}: "
                f"{people_count} people detected | "
                f"Storage status: {response.status_code}"
            )

        except Exception as error:
            print("[ERROR] Cannot send result to storage server")
            print(error)


if __name__ == "__main__":
    main()