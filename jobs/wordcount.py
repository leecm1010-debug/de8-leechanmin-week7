from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col, desc

# 클러스터 주소(--master)는 코드에 적지 않고 spark-submit 에서 지정
spark = SparkSession.builder.appName("WordCount_leechanmin").getOrCreate()

# 1) 텍스트 파일 읽기: 한 줄이 한 행 (컬럼 이름: value)
lines = spark.read.text("/opt/spark/data/wordcount.txt")

# 2) 공백 " " 기준으로 단어 분리 → explode 로 단어 하나당 한 행
#    (대소문자 변환·구두점 제거는 하지 않음)
words = lines.select(explode(split(col("value"), " ")).alias("word"))

# 빈 줄을 split 하면 빈 문자열("")이 생기므로 제외
words = words.filter(col("word") != "")

# 3) 단어별 개수 집계
word_counts = words.groupBy("word").count()

print(f"total words  = {words.count()}")
print(f"unique words = {word_counts.count()}")

# 4) 빈도 내림차순 상위 20개 (빈도가 같으면 단어 오름차순)
word_counts.orderBy(desc("count"), col("word")).show(20)

spark.stop()
