from pyspark import SparkContext, SparkConf
import os

# Cấu hình
conf = SparkConf().setAppName("Bai1_AvgRating").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

BASE     = "hdfs://localhost:9000/movie/input"
MOVIES   = f"{BASE}/movies.txt"
RATINGS1 = f"{BASE}/ratings_1.txt"
RATINGS2 = f"{BASE}/ratings_2.txt"

OUTPUT_HDFS  = "hdfs://localhost:9000/movie/output/bai1"
OUTPUT_LOCAL = "/mnt/d/UIT 3rd year/BigData/ThucHanh/Lab3/output/bai1.txt"

# Bước 1: Đọc movies.txt → map MovieID → Title
movie_titles = sc.textFile(MOVIES) \
                 .map(lambda line: line.split(",")) \
                 .map(lambda x: (int(x[0]), x[1])) \
                 .collectAsMap()

# Bước 2: Đọc ratings, map MovieID → (Rating, 1)
ratings_rdd = sc.textFile(",".join([RATINGS1, RATINGS2])) \
                .map(lambda line: line.split(",")) \
                .map(lambda x: (int(x[1]), (float(x[2]), 1)))

# Bước 3: Reduce → tính tổng điểm và số lượt
movie_ratings = ratings_rdd.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))

# Bước 4: Tính điểm trung bình, lọc >= 50 lượt
filtered = movie_ratings.filter(lambda x: x[1][1] >= 50) \
                        .map(lambda x: (x[0], round(x[1][0]/x[1][1], 4), x[1][1]))

# Sắp xếp toàn bộ danh sách theo điểm TB giảm dần
all_sorted = filtered.sortBy(lambda x: -x[1]).collect()

# Bước 5: Tìm phim có điểm TB cao nhất
best = all_sorted[0]
best_title = movie_titles.get(best[0], "Unknown")

print(f"  {'STT':<5} {'Tên phim':<50} {'Avg':>7} {'Count':>7}")
print("  " + "-"*72)
for i, (mid, avg, cnt) in enumerate(all_sorted, 1):
    title = movie_titles.get(mid, "Unknown")
    print(f"  {i:<5} {title:<50} {avg:>7.4f} {cnt:>7}")

print(f"\n>>> Phim có điểm TB cao nhất (>= 50 lượt đánh giá):")
print(f"    Tên phim  : {best_title}")
print(f"    MovieID   : {best[0]}")
print(f"    Avg Rating: {best[1]:.4f}")
print(f"    Số lượt   : {best[2]}")

lines = []
lines.append(f"\nDanh sách tất cả phim và điểm trung bình (>= 50 lượt):")
lines.append(f"  {'STT':<5} {'Tên phim':<50} {'Avg':>7} {'Count':>7}")
lines.append("  " + "-"*72)
for i, (mid, avg, cnt) in enumerate(all_sorted, 1):
    title = movie_titles.get(mid, "Unknown")
    lines.append(f"  {i:<5} {title:<50} {avg:>7.4f} {cnt:>7}")

lines.append(f"\nPhim có điểm TB cao nhất (>= 50 lượt đánh giá):")
lines.append(f"  Tên phim  : {best_title}")
lines.append(f"  MovieID   : {best[0]}")
lines.append(f"  Avg Rating: {best[1]:.4f}")
lines.append(f"  Số lượt   : {best[2]}")

# Lưu lên HDFS
all_results = sc.parallelize(all_sorted) \
                .map(lambda x: f"{movie_titles.get(x[0],'Unknown')}::{x[0]}::{x[1]}::{x[2]}")
all_results.saveAsTextFile(OUTPUT_HDFS)
print(f"\n>>> Đã lưu kết quả lên HDFS: {OUTPUT_HDFS}")

# Lưu về local
os.makedirs(os.path.dirname(OUTPUT_LOCAL), exist_ok=True)
with open(OUTPUT_LOCAL, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))
print(f">>> Đã lưu kết quả về local: {OUTPUT_LOCAL}")
print("="*60 + "\n")

sc.stop()