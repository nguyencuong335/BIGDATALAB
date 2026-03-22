-- Input: output của Bài 1
cleaned_data = LOAD '/user/cuong/bigdatalab/lab2/bai1/output'
    USING PigStorage('\t')
    AS (
        id:chararray,
        clean_review:chararray,
        aspect:chararray,
        category:chararray,
        sentiment:chararray
    );

-- =========================================
-- 1. Thống kê tần số từ và lấy top 5 từ nhiều nhất
-- =========================================
tokenized_reviews = FOREACH cleaned_data GENERATE TOKENIZE(clean_review) AS words;

all_words = FOREACH tokenized_reviews GENERATE FLATTEN(words) AS word;

valid_words = FILTER all_words BY word IS NOT NULL AND word != '';

grouped_words = GROUP valid_words BY word;

word_freq = FOREACH grouped_words GENERATE
    group AS word,
    COUNT(valid_words) AS freq;

word_freq_sorted = ORDER word_freq BY freq DESC;
top5_words = LIMIT word_freq_sorted 5;

-- =========================================
-- 2. Thống kê số bình luận theo category
-- =========================================
grouped_category = GROUP cleaned_data BY category;

category_count = FOREACH grouped_category GENERATE
    group AS category,
    COUNT(cleaned_data) AS total_comments;

category_count_sorted = ORDER category_count BY total_comments DESC;

-- =========================================
-- 3. Thống kê số bình luận theo aspect
-- =========================================
grouped_aspect = GROUP cleaned_data BY aspect;

aspect_count = FOREACH grouped_aspect GENERATE
    group AS aspect,
    COUNT(cleaned_data) AS total_comments;

aspect_count_sorted = ORDER aspect_count BY total_comments DESC;

-- =========================================
-- Hiển thị kết quả ra màn hình
-- =========================================
DUMP top5_words;
DUMP category_count_sorted;
DUMP aspect_count_sorted;

-- =========================================
-- Lưu kết quả ra HDFS
-- =========================================
STORE top5_words
INTO '/user/cuong/bigdatalab/lab2/bai2/output/top5_words'
USING PigStorage('\t');

STORE category_count_sorted
INTO '/user/cuong/bigdatalab/lab2/bai2/output/category_count'
USING PigStorage('\t');

STORE aspect_count_sorted
INTO '/user/cuong/bigdatalab/lab2/bai2/output/aspect_count'
USING PigStorage('\t');