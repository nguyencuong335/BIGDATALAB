# BIGDATALAB
There are some labs needed to finish in a university.

# Lab 3 - Apache Spark RDD với bộ dữ liệu Movie Ratings

## 1. Giới thiệu

Lab 3 sử dụng **Apache Spark** và **PySpark RDD** để xử lý bộ dữ liệu đánh giá phim.
Mục tiêu của bài lab là luyện tập các thao tác cơ bản với RDD như:

- đọc dữ liệu từ file text
- merge nhiều RDD
- dùng `map`, `flatMap`, `join`, `reduceByKey`, `sortBy`
- tính toán thống kê từ dữ liệu thực tế

Tất cả các bài trong lab đều được cài đặt bằng **PySpark** và chạy ở chế độ **local mode**.

## 2. Cấu trúc thư mục

```bash
lab3/
├── .venv/
├── bai1/
│   └── main.py
├── bai2/
│   └── main.py
├── bai3/
│   └── main.py
├── bai4/
│   └── main.py
├── bai5/
│   └── main.py
├── bai6/
│   └── main.py
├── data/
│   ├── movies.txt
│   ├── ratings_1.txt
│   ├── ratings_2.txt
│   ├── users.txt
│   └── occupation.txt
├── common.py
└── README.md
```

## 3. Yêu cầu môi trường

- Linux VM / WSL / Kali Linux
- Python 3
- Java 21
- PySpark

### Cài đặt môi trường

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk python3-pip python3-venv
```

Tạo môi trường ảo:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install pyspark
```

Kiểm tra phiên bản:

```bash
java -version
javac -version
which pyspark
which spark-submit
```

Lưu ý: cần bảo đảm `java` và `javac` cùng trỏ tới Java 21.

## 4. Bộ dữ liệu sử dụng

### `movies.txt`

Định dạng:

```text
MovieID,Title,Genres
```

Ví dụ:

```text
1010,The Godfather: Part II (1974),Crime|Drama
```

### `ratings_1.txt`, `ratings_2.txt`

Định dạng:

```text
UserID,MovieID,Rating,Timestamp
```

Ví dụ:

```text
7,1020,4.5,1577836800
```

### `users.txt`

Định dạng:

```text
UserID,Gender,Age,Occupation,ZipCode
```

Ví dụ:

```text
1,M,28,3,12345
```

### `occupation.txt`

Định dạng:

```text
OccupationID,OccupationName
```

Ví dụ:

```text
1,Programmer
2,Doctor
```

## 5. Cách chạy chương trình

Kích hoạt môi trường ảo trước:

```bash
source .venv/bin/activate
```

Chạy từng bài:

```bash
spark-submit bai1/main.py
spark-submit bai2/main.py
spark-submit bai3/main.py
spark-submit bai4/main.py
spark-submit bai5/main.py
spark-submit bai6/main.py
```

## 6. Nội dung từng bài

### Bài 1 - Điểm trung bình của từng phim

- Đọc `movies.txt`, `ratings_1.txt`, `ratings_2.txt`
- Tính điểm trung bình của từng phim
- Tính số lượt đánh giá của từng phim
- Tìm phim có điểm trung bình cao nhất với điều kiện có ít nhất 50 lượt đánh giá

Kết quả thực tế:

Bộ dữ liệu hiện tại không có phim nào đạt điều kiện `>= 50` lượt đánh giá, nên chương trình vẫn chạy đúng nhưng không có kết quả sau khi lọc.

### Bài 2 - Điểm trung bình theo thể loại phim

- Kết hợp dữ liệu phim và dữ liệu đánh giá
- Tách cột `Genres` theo dấu `|`
- Tính điểm trung bình theo từng thể loại

### Bài 3 - Điểm trung bình của mỗi phim theo giới tính

- Kết hợp `ratings` với `users`
- Lấy `Gender`
- Gom nhóm theo `(MovieID, Gender)`
- Tính điểm trung bình và số lượt đánh giá

### Bài 4 - Điểm trung bình của mỗi phim theo nhóm tuổi

- Kết hợp `ratings` với `users`
- Chuyển tuổi thành nhóm tuổi:
  - `Under 18`
  - `18-25`
  - `26-35`
  - `36-45`
  - `46-55`
  - `56+`
- Gom nhóm theo `(MovieID, AgeGroup)`
- Tính điểm trung bình và số lượt đánh giá

### Bài 5 - Điểm trung bình và tổng số lượt đánh giá theo nghề nghiệp

- Kết hợp `ratings` với `users`
- Lấy `OccupationID`
- Join với `occupation.txt` để lấy tên nghề nghiệp
- Tính điểm trung bình và tổng số lượt đánh giá cho từng nghề

### Bài 6 - Tổng số lượt đánh giá và điểm trung bình theo năm

- Đọc `Timestamp` từ dữ liệu rating
- Chuyển `Timestamp` sang `Year`
- Gom nhóm theo năm
- Tính:
  - điểm trung bình theo năm
  - tổng số lượt đánh giá theo năm

Kết quả thực tế:

Toàn bộ dữ liệu rating hiện tại đều thuộc năm 2020, nên output chỉ có 1 dòng cho năm 2020.

## 7. Một số kỹ thuật Spark RDD đã sử dụng

Trong lab này, các phép toán RDD chính được dùng gồm:

- `textFile()` để đọc dữ liệu
- `union()` để gộp 2 file rating
- `map()` để parse dữ liệu
- `flatMap()` để tách nhiều thể loại phim
- `join()` để nối giữa nhiều nguồn dữ liệu
- `reduceByKey()` để cộng tổng điểm và số lượng
- `mapValues()` để tính trung bình
- `sortBy()` / `sortByKey()` để sắp xếp kết quả


