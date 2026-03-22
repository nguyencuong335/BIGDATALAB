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
-- PHAN 1: TOP 5 TU TICH CUC NHAT THEO CATEGORY
-- =========================================

positive_reviews = FILTER cleaned_data BY sentiment == 'positive';

positive_words = FOREACH positive_reviews GENERATE
    category,
    FLATTEN(TOKENIZE(clean_review)) AS word;

positive_words_valid = FILTER positive_words BY word IS NOT NULL AND word != '';

positive_group_word = GROUP positive_words_valid BY (category, word);

positive_word_count = FOREACH positive_group_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(positive_words_valid) AS freq;

positive_group_category = GROUP positive_word_count BY category;

top5_positive_words = FOREACH positive_group_category {
    sorted_words = ORDER positive_word_count BY freq DESC, word ASC;
    top_words = LIMIT sorted_words 5;
    GENERATE FLATTEN(top_words);
};

-- =========================================
-- PHAN 2: TOP 5 TU TIEU CUC NHAT THEO CATEGORY
-- =========================================

negative_reviews = FILTER cleaned_data BY sentiment == 'negative';

negative_words = FOREACH negative_reviews GENERATE
    category,
    FLATTEN(TOKENIZE(clean_review)) AS word;

negative_words_valid = FILTER negative_words BY word IS NOT NULL AND word != '';

negative_group_word = GROUP negative_words_valid BY (category, word);

negative_word_count = FOREACH negative_group_word GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(negative_words_valid) AS freq;

negative_group_category = GROUP negative_word_count BY category;

top5_negative_words = FOREACH negative_group_category {
    sorted_words = ORDER negative_word_count BY freq DESC, word ASC;
    top_words = LIMIT sorted_words 5;
    GENERATE FLATTEN(top_words);
};

-- =========================================
-- HIEN THI KET QUA
-- =========================================
DUMP top5_positive_words;
DUMP top5_negative_words;

-- =========================================
-- LUU KET QUA RA HDFS
-- =========================================
STORE top5_positive_words
INTO '/user/cuong/bigdatalab/lab2/bai4/output/top5_positive_words_by_category'
USING PigStorage('\t');

STORE top5_negative_words
INTO '/user/cuong/bigdatalab/lab2/bai4/output/top5_negative_words_by_category'
USING PigStorage('\t');