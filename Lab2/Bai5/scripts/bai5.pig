-- Input: output cua Bai 1
cleaned_data = LOAD '/user/cuong/bigdatalab/lab2/bai1/output'
    USING PigStorage('\t')
    AS (
        id:chararray,
        clean_review:chararray,
        aspect:chararray,
        category:chararray,
        sentiment:chararray
    );

-- Tach tung tu theo category
words_by_category = FOREACH cleaned_data GENERATE
    category,
    FLATTEN(TOKENIZE(clean_review)) AS word;

valid_words = FILTER words_by_category BY word IS NOT NULL AND word != '';

-- TF: so lan xuat hien cua tu trong tung category
group_tf = GROUP valid_words BY (category, word);

word_tf = FOREACH group_tf GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(valid_words) AS tf;

-- DF: so category co chua tu do
group_df = GROUP word_tf BY word;

word_df = FOREACH group_df GENERATE
    group AS word,
    COUNT(word_tf) AS df;

-- Tong so category N
categories = GROUP cleaned_data BY category;
category_list = FOREACH categories GENERATE group AS category;

all_categories = GROUP category_list ALL;
total_categories = FOREACH all_categories GENERATE COUNT(category_list) AS total_cat;

-- Join TF + DF
tf_df_join = JOIN word_tf BY word, word_df BY word;

-- Cross voi tong so category de tinh TF-IDF
tf_df_total = CROSS tf_df_join, total_categories;

scored_words = FOREACH tf_df_total GENERATE
    word_tf::category AS category,
    word_tf::word AS word,
    word_tf::tf AS tf,
    word_df::df AS df,
    ((double)word_tf::tf) * LOG((double)total_categories::total_cat / (double)word_df::df) AS tfidf_score;

-- Lay top 5 tu lien quan nhat cho moi category
group_by_category = GROUP scored_words BY category;

top5_related_words = FOREACH group_by_category {
    sorted_words = ORDER scored_words BY tfidf_score DESC, tf DESC, word ASC;
    top_words = LIMIT sorted_words 5;
    GENERATE FLATTEN(top_words);
};

-- Hien thi ket qua
DUMP top5_related_words;

-- Luu ket qua ra HDFS
STORE top5_related_words
INTO '/user/cuong/bigdatalab/lab2/bai5/output/top5_related_words_by_category'
USING PigStorage('\t');