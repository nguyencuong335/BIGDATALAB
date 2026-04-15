# Lab 4 - Phân Tích Dữ Liệu Thương Mại với Spark DataFrame

## 1. Giới thiệu

Bài thực hành này sử dụng **PySpark DataFrame** để phân tích dữ liệu thương mại điện tử từ nhiều bảng dữ liệu CSV.
Mục tiêu là thực hiện các phép đọc dữ liệu, thống kê, gom nhóm, xử lý dữ liệu bẩn và tính toán doanh thu theo yêu cầu của đề bài.

Bộ dữ liệu gồm 5 bảng chính:

- `Customer_List.csv`: thông tin khách hàng
- `Orders.csv`: thông tin đơn hàng
- `Order_Items.csv`: chi tiết từng sản phẩm trong đơn hàng
- `Products.csv`: thông tin sản phẩm
- `Order_Reviews.csv`: thông tin đánh giá đơn hàng

## 2. Cấu trúc thư mục

```text
BIGDATALAB/
├── Lab4/
│   ├── data/
│   │   ├── Customer_List.csv
│   │   ├── Orders.csv
│   │   ├── Order_Items.csv
│   │   ├── Products.csv
│   │   └── Order_Reviews.csv
│   ├── output/
│   │   ├── cau3_orders_by_country/
│   │   ├── cau4_orders_by_year_month/
│   │   ├── cau5_review_score_distribution/
│   │   └── cau6_revenue_2024_by_category/
│   ├── screenshots/
│   │   ├── cau1/
│   │   ├── cau2/
│   │   ├── cau3/
│   │   ├── cau4/
│   │   ├── cau5/
│   │   └── cau6/
│   └── src/
│       └── lab4.py
└── README.md
```

## 3. Môi trường thực hiện

Bài được chạy trên:

- VSCode
- Linux VM
- Python 3
- PySpark
- Java 21

## 4. Cài đặt môi trường

### Bước 1: Tạo môi trường ảo

```bash
cd ~/BIGDATALAB
python3 -m venv .venv
source .venv/bin/activate
```

### Bước 2: Cài PySpark

```bash
python -m pip install --upgrade pip
python -m pip install pyspark
```

### Bước 3: Kiểm tra Java

```bash
java -version
javac -version
```

Lưu ý: Spark chạy ổn với Java 21 trong môi trường hiện tại.

Nếu cần đặt biến môi trường:

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

## 5. Cách chạy chương trình

Từ thư mục gốc `BIGDATALAB`, chạy:

```bash
cd ~/BIGDATALAB
source .venv/bin/activate
python Lab4/src/lab4.py
```

## 6. Các vấn đề kỹ thuật đã xử lý

Trong quá trình chạy bài lab, có một số vấn đề kỹ thuật và đã được xử lý như sau:

### 6.1. Spark đọc nhầm sang HDFS

Ban đầu Spark có đọc file theo `localhost:9000`, gây lỗi kết nối Hadoop.

Cách khắc phục:

- Dùng đường dẫn tuyệt đối
- Cấu hình Spark dùng local file system: `.config("spark.hadoop.fs.defaultFS", "file:///")`
- Đọc file bằng tiền tố `file://`

### 6.2. File CSV dùng dấu `;`

Tất cả file dữ liệu dùng dấu chấm phẩy `;` thay vì dấu phẩy `,`.

Cách khắc phục:

- Thêm `sep=";"` khi đọc CSV

### 6.3. File `Order_Reviews.csv` có comment nhiều dòng

Một số ô comment trong file review chứa xuống dòng, làm lệch cột khi đọc dữ liệu.

Cách khắc phục:

- Bật chế độ đọc nhiều dòng: `.option("multiLine", True)`
- Cấu hình thêm:
`.option("quote", '"')`
`.option("escape", '"')`

### 6.4. Làm sạch `Review_Score`

Cột `Review_Score` được đọc dưới dạng chuỗi, nên cần làm sạch trước khi tính toán.

Cách khắc phục:

- Chỉ giữ các giá trị hợp lệ từ 1 đến 5
- Giá trị `NULL` hoặc ngoại lệ được loại bỏ khỏi phép tính trung bình

## 7. Nội dung thực hiện theo đề bài

### Câu 1. Đọc dữ liệu từ các file CSV và tự suy ra kiểu dữ liệu

Chương trình đọc đủ 5 file dữ liệu bằng Spark DataFrame.
Các bảng `Customer_List`, `Orders`, `Order_Items`, `Products` được suy ra kiểu dữ liệu tự động.
Riêng bảng `Order_Reviews` được đọc theo chế độ phù hợp với dữ liệu nhiều dòng.

Kết quả kiểm tra số dòng:

- `Customer_List`: 102727
- `Orders`: 99441
- `Order_Items`: 112650
- `Products`: 32951
- `Order_Reviews`: 99223

### Câu 2. Thống kê tổng số đơn hàng, khách hàng và người bán

Thực hiện đếm số lượng duy nhất của các thực thể chính:

- Tổng số đơn hàng: 99441
- Tổng số khách hàng: 99382
- Tổng số người bán: 3095

Lưu ý:

- Số khách hàng được đếm theo `Subscriber_ID` để phản ánh đúng khách hàng duy nhất
- Cách này chính xác hơn so với đếm theo mã giao dịch khách hàng

### Câu 3. Phân tích số lượng đơn hàng theo quốc gia

Dữ liệu đơn hàng được nối với dữ liệu khách hàng thông qua `Customer_Trx_ID`, sau đó nhóm theo `Customer_Country`.

Một số quốc gia có số lượng đơn hàng cao:

- Germany: 41754
- France: 12848
- Netherlands: 11629
- Belgium: 5464
- Austria: 5043

Nhận xét:

- Germany là thị trường có số đơn hàng lớn nhất
- Số lượng đơn hàng tập trung mạnh ở một vài quốc gia châu Âu

### Câu 4. Phân tích số lượng đơn hàng theo năm và tháng đặt hàng

Từ cột `Order_Purchase_Timestamp`, chương trình trích xuất:

- Năm đặt hàng
- Tháng đặt hàng

Sau đó nhóm theo `(năm, tháng)` và sắp xếp:

- Năm tăng dần
- Tháng giảm dần

Nhận xét:

- Dữ liệu có đơn hàng từ năm 2022 đến 2024
- Số lượng đơn hàng tăng mạnh trong năm 2023 và 2024
- Giai đoạn đầu năm 2024 có sản lượng đơn hàng cao

### Câu 5. Thống kê điểm đánh giá trung bình và số lượng đánh giá theo từng mức

Sau khi làm sạch dữ liệu `Review_Score`, chương trình thu được:

- Tổng số review: 99223
- Số review hợp lệ: 99223
- Số review `NULL` hoặc ngoại lệ: 0
- Điểm đánh giá trung bình: 4.0864

Phân bố điểm đánh giá:

- 1 sao: 11424
- 2 sao: 3151
- 3 sao: 8179
- 4 sao: 19141
- 5 sao: 57328

Nhận xét:

- Phần lớn đánh giá tập trung ở mức 5 sao
- Điểm trung bình lớn hơn 4, cho thấy mức độ hài lòng của khách hàng khá cao

### Câu 6. Tính doanh thu năm 2024 theo danh mục sản phẩm

Doanh thu được tính theo công thức:

```text
Revenue = Price + Freight_Value
```

Dữ liệu được:

- Nối từ `Orders`, `Order_Items`, `Products`
- Lọc riêng cho năm 2024
- Nhóm theo `Product_Category_Name`

Một số danh mục có doanh thu cao nhất:

- Health_Beauty: 885191.12
- Watches_Gifts: 771986.75
- Bed_Bath_Table: 650794.70
- Sports_Leisure: 621999.34
- Computers_Accessories: 594771.04

Nhận xét:

- Nhóm sản phẩm chăm sóc sức khỏe và làm đẹp có doanh thu cao nhất
- Các nhóm quà tặng, đồ gia dụng, thể thao và phụ kiện máy tính cũng đóng góp lớn

## 8. File kết quả và minh chứng

Sau khi chạy chương trình, kết quả được ghi ra thư mục `Lab4/output/`:

- `cau3_orders_by_country`
- `cau4_orders_by_year_month`
- `cau5_review_score_distribution`
- `cau6_revenue_2024_by_category`

Ngoài ra, thư mục `Lab4/screenshots/` lưu ảnh màn hình kết quả cho từng câu để phục vụ việc báo cáo và nộp bài.

## 9. Tệp thực thi chính

Logic xử lý chính nằm trong tệp:

```text
Lab4/src/lab4.py
```

Chương trình thực hiện đầy đủ các bước:

- Khởi tạo SparkSession ở chế độ local
- Đọc và làm sạch dữ liệu
- Thực hiện thống kê theo từng câu hỏi
- Hiển thị kết quả ra màn hình
- Lưu một số bảng kết quả ra file CSV
