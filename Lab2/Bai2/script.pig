-- Load dữ liệu
raw_data = LOAD '/home/linux_user/pig_lab2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- Chuyển chữ viết thường
data = FOREACH raw_data GENERATE
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

-- Tần số xuất hiện mỗi từ
word_groups = GROUP result_1 BY word;
word_freq = FOREACH word_groups GENERATE
    group AS word,
    COUNT(result_1) AS freq;

-- Chỉ lấy từ xuất hiện trên 500 lần
freq_gt500 = FILTER word_freq BY freq > 500;
freq_sorted = ORDER freq_gt500 BY freq DESC;

STORE freq_sorted INTO '/home/linux_user/pig_lab2/output/bai2_1_word_freq' 
    USING PigStorage('\t');

-- Thống kê số bình luận theo topic
topic_groups = GROUP raw_data BY topic;
topic_count = FOREACH topic_groups GENERATE
    group AS topic,
    COUNT(raw_data) AS num_comments;
topic_sorted = ORDER topic_count BY num_comments DESC;
STORE topic_sorted INTO '/home/linux_user/pig_lab2/output/bai2_2_topic_count' 
    USING PigStorage('\t');

-- Thống kê số bình luận theo aspect
aspect_group = GROUP raw_data BY aspect;
aspect_count = FOREACH aspect_group GENERATE
    group AS aspect,
    COUNT(raw_data) AS num_comments;
aspect_sorted = ORDER aspect_count BY num_comments DESC;
STORE aspect_sorted INTO '/home/linux_user/pig_lab2/output/bai2_3_aspect_count' 
    USING PigStorage('\t');
