-- Input: sử dụng output từ Bài 1
cleaned_data = LOAD '/user/cuong/bigdatalab/lab2/bai1/output'
    USING PigStorage('\t')
    AS (
        id:chararray,
        clean_review:chararray,
        aspect:chararray,
        category:chararray,
        sentiment:chararray
    );

-- Nhóm theo aspect và sentiment
grouped_data = GROUP cleaned_data BY (aspect, sentiment);

aspect_sentiment_count = FOREACH grouped_data GENERATE
    group.aspect AS aspect,
    group.sentiment AS sentiment,
    COUNT(cleaned_data) AS total_reviews;

-- Tách riêng negative
negative_data = FILTER aspect_sentiment_count BY sentiment == 'negative';
negative_sorted = ORDER negative_data BY total_reviews DESC;
top_negative = LIMIT negative_sorted 1;

-- Tách riêng positive
positive_data = FILTER aspect_sentiment_count BY sentiment == 'positive';
positive_sorted = ORDER positive_data BY total_reviews DESC;
top_positive = LIMIT positive_sorted 1;

-- Hiển thị kết quả ra màn hình
DUMP top_negative;
DUMP top_positive;

-- Lưu kết quả ra HDFS
STORE top_negative
INTO '/user/cuong/bigdatalab/lab2/bai3/output/top_negative_aspect'
USING PigStorage('\t');

STORE top_positive
INTO '/user/cuong/bigdatalab/lab2/bai3/output/top_positive_aspect'
USING PigStorage('\t');