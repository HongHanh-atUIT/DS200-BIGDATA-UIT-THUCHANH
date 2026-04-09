-- Load từ bài 1
data = LOAD '/home/linux_user/pig_lab2/output/bai1'
    USING PigStorage('\t')
    AS (word:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- TF: số lần word xuất hiện trong topic
grp_tf = GROUP data BY (topic, word);

tf = FOREACH grp_tf GENERATE
    group.topic AS topic,
    group.word AS word,
    COUNT(data) AS tf;

-- DF: số topic chứa word
grp_df = GROUP data BY (word, topic);
distinct_word_topic = FOREACH grp_df GENERATE
    group.word AS word,
    group.topic AS topic;

grp_df2 = GROUP distinct_word_topic BY word;

df = FOREACH grp_df2 GENERATE
    group AS word,
    COUNT(distinct_word_topic) AS df;

-- Tổng số topic (N)
topics = GROUP data BY topic;
topics = FOREACH topics GENERATE group AS topic;

all_topics = GROUP topics ALL;
N_rel = FOREACH all_topics GENERATE COUNT(topics) AS N;

-- JOIN TF + DF + N
tf_df = JOIN tf BY word, df BY word;
tf_df_n = CROSS tf_df, N_rel;

-- Tính TF-IDF
tfidf = FOREACH tf_df_n GENERATE
    tf::topic AS topic,
    tf::word AS word,
    (double)tf::tf * LOG((double)N / (double)df::df) AS score;

-- Lấy top 5 mỗi topic
grp_topic = GROUP tfidf BY topic;

top5 = FOREACH grp_topic {
    sorted = ORDER tfidf BY score DESC;
    top = LIMIT sorted 5;
    GENERATE
        group AS topic,
        FLATTEN(top) AS (t:chararray, word:chararray, score:double);
};

-- bỏ cột t dư
result = FOREACH top5 GENERATE
    topic,
    word,
    score;

STORE result INTO '/home/linux_user/pig_lab2/output/bai5_tfidf'
    USING PigStorage('\t');