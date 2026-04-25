from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("Bai2_GenreRating").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

BASE     = "hdfs://localhost:9000/movie/input"
MOVIES   = f"{BASE}/movies.txt"
RATINGS1 = f"{BASE}/ratings_1.txt"
RATINGS2 = f"{BASE}/ratings_2.txt"

OUTPUT_HDFS  = "hdfs://localhost:9000/movie/output/bai2"
OUTPUT_LOCAL = "/mnt/d/UIT 3rd year/BigData/ThucHanh/Lab3/output/bai2.txt"


# Bước 1: Tạo map MovieID → List of Genres
genre_map = sc.textFile(MOVIES) \
              .map(lambda line: line.split(",")) \
              .map(lambda x: (int(x[0]), x[2].strip().split("|"))) \
              .collectAsMap()
genre_bc = sc.broadcast(genre_map)

# Bước 2: Map MovieID → Rating → (Genre, Rating)
ratings_rdd = sc.textFile(",".join([RATINGS1, RATINGS2])) \
                .map(lambda line: line.split(","))

genre_ratings = ratings_rdd.flatMap(lambda x: [
    (g, (float(x[2]), 1))
    for g in genre_bc.value.get(int(x[1]), [])
])

# Bước 3: Tính trung bình điểm đánh giá cho từng thể loại
result = genre_ratings.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                      .map(lambda x: (x[0], round(x[1][0]/x[1][1], 4), x[1][1])) \
                      .sortBy(lambda x: -x[1]) \
                      .collect()

print(f"\n  {'STT':<5} {'Thể loại':<25} {'Avg Rating':>10} {'Số lượt':>10}")
print("  " + "-"*52)
for i, (genre, avg, cnt) in enumerate(result, 1):
    print(f"  {i:<5} {genre:<25} {avg:>10.4f} {cnt:>10}")

lines = []
lines.append(f"\n  {'STT':<5} {'Thể loại':<25} {'Avg Rating':>10} {'Số lượt':>10}")
lines.append("  " + "-"*52)
for i, (genre, avg, cnt) in enumerate(result, 1):
    lines.append(f"  {i:<5} {genre:<25} {avg:>10.4f} {cnt:>10}")

# Lưu lên HDFS
result_rdd = sc.parallelize(result) \
               .map(lambda x: f"{x[0]}::{x[1]}::{x[2]}")
result_rdd.saveAsTextFile(OUTPUT_HDFS)
print(f"\n>>> Đã lưu kết quả lên HDFS: {OUTPUT_HDFS}")

# Lưu về local
os.makedirs(os.path.dirname(OUTPUT_LOCAL), exist_ok=True)
with open(OUTPUT_LOCAL, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f">>> Đã lưu kết quả về local: {OUTPUT_LOCAL}")
print("="*60 + "\n")

sc.stop()