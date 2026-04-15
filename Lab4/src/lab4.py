from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, year, month, avg, when,
    sum as _sum, trim, regexp_extract, round as _round
)

# ==========================================================
# KHỞI TẠO SPARK
# - Chạy local trên máy
# - Ép Spark dùng file local thay vì HDFS
# ==========================================================
spark = SparkSession.builder \
    .appName("Lab4_Spark_DataFrame") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Giảm bớt log để output gọn hơn
spark.sparkContext.setLogLevel("ERROR")

# ==========================================================
# KHAI BÁO ĐƯỜNG DẪN DỮ LIỆU
# Lưu ý:
# - Đang dùng đường dẫn tuyệt đối để tránh Spark hiểu nhầm sang HDFS
# - Nếu username máy bạn khác "cuong" thì sửa lại cho đúng
# ==========================================================
base_path = "/home/cuong/BIGDATALAB/Lab4/data"

customer_path = f"{base_path}/Customer_List.csv"
orders_path = f"{base_path}/Orders.csv"
order_items_path = f"{base_path}/Order_Items.csv"
products_path = f"{base_path}/Products.csv"
reviews_path = f"{base_path}/Order_Reviews.csv"

# ==========================================================
# CÂU 1. ĐỌC DỮ LIỆU TỪ CÁC FILE CSV
# Yêu cầu:
# - Đọc dữ liệu từ các file csv
# - Tự suy ra kiểu dữ liệu cho mỗi cột
#
# Lưu ý:
# - Các file csv dùng dấu phân cách ;
# - File Order_Reviews.csv có comment nhiều dòng,
#   nên phải bật multiLine=True
# ==========================================================

# Đọc bảng khách hàng
customers_df = spark.read.csv(
    f"file://{customer_path}",
    header=True,
    inferSchema=True,
    sep=";"
)

# Đọc bảng đơn hàng
orders_df = spark.read.csv(
    f"file://{orders_path}",
    header=True,
    inferSchema=True,
    sep=";"
)

# Đọc bảng chi tiết đơn hàng
order_items_df = spark.read.csv(
    f"file://{order_items_path}",
    header=True,
    inferSchema=True,
    sep=";"
)

# Đọc bảng sản phẩm
products_df = spark.read.csv(
    f"file://{products_path}",
    header=True,
    inferSchema=True,
    sep=";"
)

# Đọc bảng đánh giá
# Bật multiLine để xử lý comment nhiều dòng
reviews_df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .option("sep", ";") \
    .option("multiLine", True) \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(f"file://{reviews_path}")

print("\n================ CÂU 1: SCHEMA CÁC BẢNG ================")

print("\nSchema của bảng Customer_List:")
customers_df.printSchema()

print("\nSchema của bảng Orders:")
orders_df.printSchema()

print("\nSchema của bảng Order_Items:")
order_items_df.printSchema()

print("\nSchema của bảng Products:")
products_df.printSchema()

print("\nSchema của bảng Order_Reviews:")
reviews_df.printSchema()

# Kiểm tra nhanh số dòng từng bảng
print("\n================ KIỂM TRA NHANH SỐ DÒNG MỖI BẢNG ================")
print(f"So dong Customer_List: {customers_df.count()}")
print(f"So dong Orders: {orders_df.count()}")
print(f"So dong Order_Items: {order_items_df.count()}")
print(f"So dong Products: {products_df.count()}")
print(f"So dong Order_Reviews: {reviews_df.count()}")

# ==========================================================
# CÂU 2. THỐNG KÊ TỔNG SỐ ĐƠN HÀNG, SỐ LƯỢNG KHÁCH HÀNG,
#         VÀ SỐ LƯỢNG NGƯỜI BÁN
#
# Ghi chú:
# - Tổng số đơn hàng: đếm Order_ID trong Orders
# - Số lượng khách hàng: đếm Subscriber_ID để sát nghĩa
#   "khách hàng duy nhất" hơn
# - Số lượng người bán: đếm Seller_ID trong Order_Items
# ==========================================================
total_orders = orders_df.select(
    countDistinct("Order_ID").alias("Tong_so_don_hang")
)

total_customers = customers_df.select(
    countDistinct("Subscriber_ID").alias("Tong_so_khach_hang")
)

total_sellers = order_items_df.select(
    countDistinct("Seller_ID").alias("Tong_so_nguoi_ban")
)

print("\n================ CÂU 2: THỐNG KÊ TỔNG QUAN ================")
total_orders.show()
total_customers.show()
total_sellers.show()

# ==========================================================
# CÂU 3. PHÂN TÍCH SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA
# Sắp xếp theo thứ tự giảm dần
#
# Cách làm:
# - Join Orders với Customer_List qua Customer_Trx_ID
# - Group theo Customer_Country
# - Đếm số lượng đơn hàng
# - Sắp xếp giảm dần
# ==========================================================
orders_by_country = orders_df.join(
    customers_df,
    on="Customer_Trx_ID",
    how="inner"
).groupBy(
    "Customer_Country"
).agg(
    countDistinct("Order_ID").alias("So_luong_don_hang")
).orderBy(
    col("So_luong_don_hang").desc()
)

print("\n================ CÂU 3: SỐ LƯỢNG ĐƠN HÀNG THEO QUỐC GIA ================")
orders_by_country.show(50, truncate=False)

# ==========================================================
# CÂU 4. PHÂN TÍCH SỐ LƯỢNG ĐƠN HÀNG NHÓM THEO NĂM, THÁNG ĐẶT HÀNG
# Yêu cầu:
# - Hiển thị theo năm tăng dần
# - Tháng giảm dần
# ==========================================================
orders_by_year_month = orders_df.withColumn(
    "Nam_dat_hang", year(col("Order_Purchase_Timestamp"))
).withColumn(
    "Thang_dat_hang", month(col("Order_Purchase_Timestamp"))
).groupBy(
    "Nam_dat_hang", "Thang_dat_hang"
).agg(
    countDistinct("Order_ID").alias("So_luong_don_hang")
).orderBy(
    col("Nam_dat_hang").asc(),
    col("Thang_dat_hang").desc()
)

print("\n================ CÂU 4: SỐ LƯỢNG ĐƠN HÀNG THEO NĂM, THÁNG ================")
orders_by_year_month.show(100, truncate=False)

# ==========================================================
# CÂU 5. THỐNG KÊ ĐIỂM ĐÁNH GIÁ TRUNG BÌNH, SỐ LƯỢNG ĐÁNH GIÁ
# THEO TỪNG MỨC (1 ĐẾN 5)
#
# Yêu cầu đề bài:
# - Cần xử lý NULL
# - Cần xử lý giá trị ngoại lệ trong Review_Score
#
# Cách làm:
# - Review_Score hiện đang ở dạng string
# - Chỉ giữ các giá trị hợp lệ đúng là 1, 2, 3, 4, 5
# - Giá trị khác sẽ đưa về NULL
# ==========================================================
reviews_clean_df = reviews_df.withColumn(
    "Review_Score_Text",
    trim(col("Review_Score"))
).withColumn(
    "Review_Score_Num_Text",
    regexp_extract(col("Review_Score_Text"), r"^[1-5]$", 0)
).withColumn(
    "Review_Score_Clean",
    when(
        col("Review_Score_Num_Text") != "",
        col("Review_Score_Num_Text").cast("int")
    ).otherwise(None)
)

# Thống kê số lượng review hợp lệ và không hợp lệ
review_quality_stats = reviews_clean_df.select(
    count("*").alias("Tong_so_review"),
    count("Review_Score_Clean").alias("So_review_hop_le")
)

invalid_review_stats = reviews_clean_df.filter(
    col("Review_Score_Clean").isNull()
).select(
    count("*").alias("So_review_null_hoac_ngoai_le")
)

# Tính điểm đánh giá trung bình
avg_review_score = reviews_clean_df.select(
    _round(avg("Review_Score_Clean"), 4).alias("Diem_danh_gia_trung_binh")
)

# Đếm số lượng đánh giá theo từng mức
review_score_distribution = reviews_clean_df.filter(
    col("Review_Score_Clean").isNotNull()
).groupBy(
    "Review_Score_Clean"
).agg(
    count("*").alias("So_luong_danh_gia")
).orderBy(
    "Review_Score_Clean"
)

print("\n================ CÂU 5: KIỂM TRA DỮ LIỆU REVIEW SAU KHI LÀM SẠCH ================")
review_quality_stats.show()
invalid_review_stats.show()

print("\n================ CÂU 5: ĐIỂM ĐÁNH GIÁ TRUNG BÌNH ================")
avg_review_score.show()

print("\n================ CÂU 5: SỐ LƯỢNG ĐÁNH GIÁ THEO TỪNG MỨC ================")
review_score_distribution.show()

# ==========================================================
# CÂU 6. TÍNH DOANH THU TRONG NĂM 2024 VÀ NHÓM THEO DANH MỤC SẢN PHẨM
#
# Công thức:
# - Revenue = Price + Freight_Value
#
# Cách làm:
# - Join Orders với Order_Items theo Order_ID
# - Join tiếp với Products theo Product_ID
# - Lọc các đơn hàng trong năm 2024
# - Tính doanh thu từng dòng
# - Group theo Product_Category_Name
# - Làm tròn 2 chữ số thập phân
# ==========================================================
revenue_2024_by_category = orders_df.join(
    order_items_df,
    on="Order_ID",
    how="inner"
).join(
    products_df,
    on="Product_ID",
    how="inner"
).filter(
    year(col("Order_Purchase_Timestamp")) == 2024
).withColumn(
    "Revenue",
    col("Price") + col("Freight_Value")
).groupBy(
    "Product_Category_Name"
).agg(
    _round(_sum("Revenue"), 2).alias("Tong_doanh_thu_nam_2024")
).orderBy(
    col("Tong_doanh_thu_nam_2024").desc()
)

print("\n================ CÂU 6: DOANH THU NĂM 2024 THEO DANH MỤC SẢN PHẨM ================")
revenue_2024_by_category.show(100, truncate=False)

# ==========================================================
# LƯU KẾT QUẢ RA FILE CSV NẾU CẦN NỘP
# ==========================================================
orders_by_country.coalesce(1).write.mode("overwrite").option("header", True).csv("Lab4/output/cau3_orders_by_country")
orders_by_year_month.coalesce(1).write.mode("overwrite").option("header", True).csv("Lab4/output/cau4_orders_by_year_month")
review_score_distribution.coalesce(1).write.mode("overwrite").option("header", True).csv("Lab4/output/cau5_review_score_distribution")
revenue_2024_by_category.coalesce(1).write.mode("overwrite").option("header", True).csv("Lab4/output/cau6_revenue_2024_by_category")

# ==========================================================
# KẾT THÚC CHƯƠNG TRÌNH
# ==========================================================
spark.stop()