-- Load dữ liệu
raw_data = LOAD '/home/linux_user/pig_lab2/data/hotel-review.csv'
    USING PigStorage(';')
    AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- Bình luận tiêu cực
negatives = FILTER raw_data BY sentiment == 'negative';
neg_asp_group = GROUP negatives BY aspect;
neg_asp_count = FOREACH neg_asp_group GENERATE
    group AS aspect,
    COUNT(negatives) AS neg_count;
neg_all = GROUP neg_asp_count ALL;
neg_max = FOREACH neg_all GENERATE
    MAX(neg_asp_count.neg_count) AS max_count;
neg_joined = JOIN neg_asp_count BY neg_count, neg_max BY max_count;
neg_top1 = FOREACH neg_joined GENERATE
    neg_asp_count::aspect AS aspect,
    neg_asp_count::neg_count AS neg_count;
STORE neg_top1 INTO '/home/linux_user/pig_lab2/output/bai3_1_negative'
    USING PigStorage('\t');

-- Bình luận tích cực
positives = FILTER raw_data BY sentiment == 'positive';
pos_asp_group = GROUP positives BY aspect;
pos_asp_count = FOREACH pos_asp_group GENERATE
    group AS aspect,
    COUNT(positives) AS pos_count;
pos_all = GROUP pos_asp_count ALL;
pos_max = FOREACH pos_all GENERATE
    MAX(pos_asp_count.pos_count) AS max_count;
pos_joined = JOIN pos_asp_count BY pos_count, pos_max BY max_count;
pos_top1 = FOREACH pos_joined GENERATE
    pos_asp_count::aspect AS aspect,
    pos_asp_count::pos_count AS pos_count;
STORE pos_top1 INTO '/home/linux_user/pig_lab2/output/bai3_2_positive'
    USING PigStorage('\t');