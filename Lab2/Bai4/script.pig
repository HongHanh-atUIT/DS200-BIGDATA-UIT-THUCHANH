-- Load từ bài 1
data = LOAD '/home/linux_user/pig_lab2/output/bai1'
    USING PigStorage('\t')
    AS (word:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- POSITIVE
pos = FILTER data BY sentiment == 'positive';

grp_pos = GROUP pos BY (topic, word);

cnt_pos = FOREACH grp_pos GENERATE
    group.topic AS topic,
    group.word AS word,
    COUNT(pos) AS freq;

grp_topic_pos = GROUP cnt_pos BY topic;

result_pos = FOREACH grp_topic_pos {
    sorted = ORDER cnt_pos BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE
        group AS topic,
        FLATTEN(top5) AS (t:chararray, word:chararray, freq:long);
};

-- bỏ cột t (topic bị lặp)
result_pos = FOREACH result_pos GENERATE
    topic,
    word,
    freq;

STORE result_pos INTO '/home/linux_user/pig_lab2/output/bai4_positive'
    USING PigStorage('\t');

-- NEGATIVE
neg = FILTER data BY sentiment == 'negative';

grp_neg = GROUP neg BY (topic, word);

cnt_neg = FOREACH grp_neg GENERATE
    group.topic AS topic,
    group.word AS word,
    COUNT(neg) AS freq;

grp_topic_neg = GROUP cnt_neg BY topic;

result_neg = FOREACH grp_topic_neg {
    sorted = ORDER cnt_neg BY freq DESC;
    top5 = LIMIT sorted 5;
    GENERATE
        group AS topic,
        FLATTEN(top5) AS (t:chararray, word:chararray, freq:long);
};

result_neg = FOREACH result_neg GENERATE
    topic,
    word,
    freq;

STORE result_neg INTO '/home/linux_user/pig_lab2/output/bai4_negative'
    USING PigStorage('\t');