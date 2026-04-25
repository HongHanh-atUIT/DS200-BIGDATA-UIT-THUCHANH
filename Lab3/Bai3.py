from pyspark import SparkContext, SparkConf
import os
from collections import defaultdict

conf = SparkConf().setAppName("Bai3_GenderRating").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

BASE     = "hdfs://localhost:9000/movie/input"
MOVIES   = f"{BASE}/movies.txt"
RATINGS1 = f"{BASE}/ratings_1.txt"
RATINGS2 = f"{BASE}/ratings_2.txt"
USERS    = f"{BASE}/users.txt"

OUTPUT_HDFS  = "hdfs://localhost:9000/movie/output/bai3"
OUTPUT_LOCAL = "/mnt/d/UIT 3rd year/BigData/ThucHanh/Lab3/output/bai3.txt"

# Bước 1: Tạo map UserID → Gender
gender_map = sc.textFile(USERS) \
               .map(lambda line: line.split(",")) \
               .map(lambda x: (int(x[0]), x[1])) \
               .collectAsMap()
gender_bc = sc.broadcast(gender_map)

# Lấy title phim
movie_titles = sc.textFile(MOVIES) \
                 .map(lambda line: line.split(",", 2)) \
                 .map(lambda x: (int(x[0]), x[1])) \
                 .collectAsMap()

# Bước 2: Join với ratings để thêm thông tin giới tính
ratings_rdd = sc.textFile(",".join([RATINGS1, RATINGS2])) \
                .map(lambda line: line.split(","))

# Bước 3: Tính trung bình rating cho mỗi phim theo từng giới tính
raw_result = ratings_rdd.map(lambda x: (
                        (int(x[1]), gender_bc.value.get(int(x[0]), "Unknown")),
                        (float(x[2]), 1)
                    )) \
                    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                    .map(lambda x: (x[0][0], x[0][1], round(x[1][0]/x[1][1], 4), x[1][1])) \
                    .collect()

# Nhóm theo MovieID
movie_gender_data = defaultdict(dict)
for mid, gender, avg, cnt in raw_result:
    movie_gender_data[mid][gender] = (avg, cnt)

# Fill thiếu giới tính → avg=NaN, count=0
all_genders = ["F", "M"]
result = []
for mid in sorted(movie_gender_data.keys()):
    for gender in all_genders:
        if gender in movie_gender_data[mid]:
            avg, cnt = movie_gender_data[mid][gender]
        else:
            avg, cnt = float('nan'), 0
        result.append((mid, gender, avg, cnt))

# ============================================================
# IN KẾT QUẢ
# ============================================================
print(f"\n  {'MovieID':<10} {'Tên phim':<40} {'Gender':<8} {'Avg':>7} {'Count':>7}")
print("  " + "-"*74)
for mid, gender, avg, cnt in result:
    title = movie_titles.get(mid, "Unknown")
    avg_str = f"{avg:>7.4f}" if cnt > 0 else f"{'NaN':>7}"
    print(f"  {mid:<10} {title:<40} {gender:<8} {avg_str} {cnt:>7}")

lines = []
lines.append(f"\n  {'MovieID':<10} {'Tên phim':<40} {'Gender':<8} {'Avg':>7} {'Count':>7}")
lines.append("  " + "-"*74)
for mid, gender, avg, cnt in result:
    title = movie_titles.get(mid, "Unknown")
    avg_str = f"{avg:>7.4f}" if cnt > 0 else f"{'NaN':>7}"
    lines.append(f"  {mid:<10} {title:<40} {gender:<8} {avg_str} {cnt:>7}")


# Lưu lên HDFS
result_rdd = sc.parallelize(result) \
               .map(lambda x: f"{x[0]},{movie_titles.get(x[0],'Unknown')},{x[1]},{x[2]},{x[3]}")
result_rdd.saveAsTextFile(OUTPUT_HDFS)
print(f"\n>>> Đã lưu kết quả lên HDFS: {OUTPUT_HDFS}")

# Lưu về local
os.makedirs(os.path.dirname(OUTPUT_LOCAL), exist_ok=True)
with open(OUTPUT_LOCAL, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f">>> Đã lưu kết quả về local: {OUTPUT_LOCAL}")
print("="*75 + "\n")

sc.stop()