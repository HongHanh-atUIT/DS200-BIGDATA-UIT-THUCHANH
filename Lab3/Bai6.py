from pyspark import SparkContext, SparkConf
import datetime
import os

conf = SparkConf().setAppName("Bai6_TimeRating").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

BASE     = "hdfs://localhost:9000/movie/input"
RATINGS1 = f"{BASE}/ratings_1.txt"
RATINGS2 = f"{BASE}/ratings_2.txt"

OUTPUT_HDFS  = "hdfs://localhost:9000/movie/output/bai6"
OUTPUT_LOCAL = "/mnt/d/UIT 3rd year/BigData/ThucHanh/Lab3/output/bai6.txt"

def timestamp_to_year(ts):
    return datetime.datetime.fromtimestamp(int(ts)).year

# Đọc dữ liệu ratings từ cả 2 file
ratings_rdd = sc.textFile(",".join([RATINGS1, RATINGS2])) \
                .map(lambda line: line.split(","))

# Với mỗi dòng, phát hành key = năm, value = (rating, 1)
result = ratings_rdd.map(lambda x: (
                        timestamp_to_year(x[3]),
                        (float(x[2]), 1)
                    )) \
                    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                    .map(lambda x: (x[0], round(x[1][0]/x[1][1], 4), x[1][1])) \
                    .sortBy(lambda x: x[0]) \
                    .collect()


print(f"\n  {'Năm':<8} {'Avg Rating':>12} {'Tổng lượt':>12}")
print("  " + "-"*34)
for year, avg, cnt in result:
    print(f"  {year:<8} {avg:>12.4f} {cnt:>12}")

lines = []
lines.append(f"\n  {'Năm':<8} {'Avg Rating':>12} {'Tổng lượt':>12}")
lines.append("  " + "-"*34)
for year, avg, cnt in result:
    lines.append(f"  {year:<8} {avg:>12.4f} {cnt:>12}")

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
print("="*55 + "\n")

sc.stop()