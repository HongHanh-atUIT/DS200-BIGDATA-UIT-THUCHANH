-- Load dữ liệu
data = LOAD '/home/linux_user/pig_lab2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- Chuyển chữ viết thường
data = FOREACH data GENERATE
    LOWER(review) AS review,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Bỏ dấu câu 
data = FOREACH data GENERATE
    REPLACE(review, '[^a-zA-ZÀ-ỹà-ỹ\\s]', ' ') AS review,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Tách khoảng trắng
words = FOREACH data GENERATE
    FLATTEN(TOKENIZE(review)) AS word,
    topic AS topic,
    aspect AS aspect,
    sentiment AS sentiment;

-- Bỏ stopwords
stopwords = LOAD '/home/linux_user/pig_lab2/data/stopwords.txt'
    USING PigStorage('\n')
    AS (word:chararray);

joined = JOIN words BY word LEFT OUTER, stopwords by word;
words = FILTER joined BY stopwords::word is NULL;

result_1 = FOREACH words GENERATE
    words::word AS word,
    words::topic AS topic,
    words::aspect AS aspect,
    words::sentiment AS sentiment;

STORE result_1 INTO '/home/linux_user/pig_lab2/output/bai1' 
    USING PigStorage('\t');