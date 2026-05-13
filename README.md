### Lab 5 - People Counting System

#### 1. Mô tả bài làm

Project này xây dựng hệ thống đếm số lượng người xuất hiện trong video/camera.

Hệ thống sẽ:

- Đọc frame từ video.
- Gửi frame qua Kafka.
- Dùng YOLOv8 để nhận diện người.
- Vẽ bounding box quanh người được phát hiện.
- Lưu kết quả vào PostgreSQL.
- Cung cấp API để xem kết quả đã lưu.

---

#### 2. Kiến trúc hệ thống

Hệ thống gồm 3 server chính:

##### Camera Server

Đọc từng frame từ video và gửi frame đó vào Kafka.

##### Processing Server

Nhận frame từ Kafka, dùng YOLOv8 để nhận diện người, đếm số lượng người và tạo bounding box.

##### Storage Server

Nhận kết quả từ Processing Server và lưu vào PostgreSQL.

#### 3. Công nghệ sử dụng

- Python
- Docker Compose
- Apache Kafka
- PostgreSQL
- FastAPI
- OpenCV
- YOLOv8
- SQLAlchemy

#### 4. Cách chạy project.

**Chạy lệnh:**
```bash
docker compose up --build
```

Khi hệ thống chạy thành công, terminal sẽ hiển thị dạng:
```bash
[PROCESSING SERVER] Frame 60: 11 people detected | Storage status: 200
```

**Sau đó kiểm tra API:**

Mở terminal mới và chạy:
```bash
curl http://localhost:8000/results
```

**Kết quả ảnh bounding box** được lưu vào trong output.

Sau đó, **dừng chương trình:**
```bash
docker compose down
```
