REGISTER 'build/preprocess.jar';

DEFINE CLEAN bigdata.CleanReview('/user/cuong/bigdatalab/lab2/bai1/input/stopwords.txt');

raw_data = LOAD '/user/cuong/bigdatalab/lab2/bai1/input/hotel-review.csv'
    USING PigStorage(';')
    AS (
        id:chararray,
        review:chararray,
        aspect:chararray,
        category:chararray,
        sentiment:chararray
    );

data_no_header = FILTER raw_data BY id != 'id';

cleaned_data = FOREACH data_no_header GENERATE
    id,
    CLEAN(review) AS clean_review,
    aspect,
    category,
    sentiment;

DUMP cleaned_data;

STORE cleaned_data
INTO '/user/cuong/bigdatalab/lab2/bai1/output'
USING PigStorage('\t');