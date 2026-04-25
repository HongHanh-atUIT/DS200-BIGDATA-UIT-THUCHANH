from pyspark import SparkContext, SparkConf
import os

conf = SparkConf().setAppName("Bai5_OccupationRating").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

BASE     = "hdfs://localhost:9000/movie/input"
RATINGS1 = f"{BASE}/ratings_1.txt"
RATINGS2 = f"{BASE}/ratings_2.txt"
USERS    = f"{BASE}/users.txt"
OCC      = f"{BASE}/occupation.txt"

OUTPUT_HDFS  = "hdfs://localhost:9000/movie/output/bai5"
OUTPUT_LOCAL = "/mnt/d/UIT 3rd year/BigData/ThucHanh/Lab3/output/bai5.txt"

# Tạo dictionary ID → Occupation name từ occupation.txt
occ_map = sc.textFile(OCC) \
            .map(lambda line: line.split(",")) \
            .map(lambda x: (int(x[0]), x[1].strip())) \
            .collectAsMap()

# Tạo dictionary UserID → Occupation từ users.txt
user_occ = sc.textFile(USERS) \
             .map(lambda line: line.split(",")) \
             .map(lambda x: (int(x[0]), occ_map.get(int(x[3]), "unknown"))) \
             .collectAsMap()
user_bc = sc.broadcast(user_occ)

# Với mỗi rating, gán Occupation theo UserID
# Phát hành cặp key-value: key = Occupation, value = (rating, 1)
ratings_rdd = sc.textFile(",".join([RATINGS1, RATINGS2])) \
                .map(lambda line: line.split(","))

result = ratings_rdd.map(lambda x: (
                        user_bc.value.get(int(x[0]), "unknown"),
                        (float(x[2]), 1)
                    )) \
                    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                    .map(lambda x: (x[0], round(x[1][0]/x[1][1], 4), x[1][1])) \
                    .sortBy(lambda x: -x[1]) \
                    .collect()

print(f"\n  {'STT':<5} {'Occupation':<30} {'Avg Rating':>10} {'Số lượt':>10}")
print("  " + "-"*57)
for i, (occ, avg, cnt) in enumerate(result, 1):
    print(f"  {i:<5} {occ:<30} {avg:>10.4f} {cnt:>10}")

lines = []
lines.append(f"\n  {'STT':<5} {'Occupation':<30} {'Avg Rating':>10} {'Số lượt':>10}")
lines.append("  " + "-"*57)
for i, (occ, avg, cnt) in enumerate(result, 1):
    lines.append(f"  {i:<5} {occ:<30} {avg:>10.4f} {cnt:>10}")

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