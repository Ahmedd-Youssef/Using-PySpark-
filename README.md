📘 PySpark Task
This project demonstrates various PySpark operations using both RDDs and DataFrames. It covers data creation, transformation, aggregation, filtering, and text processing.

🔧 Requirements
Python 3.x

PySpark

NumPy

SparkSession initialized

🚀 RDD Operations
➤ Create RDD from numbers 1 to 49
python
RDD = sc.parallelize(np.arange(1,50))
➤ Basic statistics
Sum: RDD.sum() → 1225

Average: RDD.mean() → 25.0

Max: RDD.max() → 49

Min: RDD.min() → 1

Count: RDD.count() → 49

➤ Count even vs. odd numbers
python
Odd = RDD.filter(lambda x:x % 2 != 0)
Even = RDD.filter(lambda x:x % 2 == 0)
👥 People Data RDD
python
people_data = [("Nada", 25), ("Mona", 30), ("Ahmed", 35), ("Khaled", 40), ("Ahmed", 35), ("Nada", 25)]
➤ Oldest person
python
rdd_people.max(key=lambda x: x[1])
➤ Average age
python
rdd_people.map(lambda x: x[1]).mean()
➤ Group names by age
python
rdd_people.groupBy(lambda x: x[1]).map(lambda x: (x[0], list(x[1])))
📄 Text RDD Analysis
➤ Load text file
python
rdd_russia = sc.textFile("/data/russia.txt")
➤ Line count: rdd_russia.count() → 8
➤ Lines containing "Russia": rdd_russia.filter(lambda line: "Russia" in line).count() → 6
➤ Top 5 frequent words
python
rdd_russia.flatMap(lambda line: line.split())
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a + b)
          .sortBy(lambda x: x[1], ascending=False)
          .take(5)
➤ Tokenization and stopword removal
python
stopwords = ['a', 'the', 'is', 'to', 'in', 'of']
rdd_russia.flatMap(lambda line: line.split())
          .filter(lambda word: word not in stopwords)
📊 DataFrame Operations
➤ Create DataFrame
python
schema = 'id integer, name string, age integer, salary integer'
➤ Show schema and first rows
python
df.printSchema()
df.show(2)
➤ Select columns
python
df.select('name', 'salary')
➤ Average salary
python
df.agg(fun.avg('salary'))
➤ Filter age > 28
python
df.where('age > 28')
➤ Count distinct names
python
df.select('name').distinct().count()
➤ Group by name and average salary
python
df.groupBy('name').avg('salary')
🧼 Handling Nulls
➤ Load CSV with nulls
python
df1 = spark.read.csv("/data/NullData.csv", header=True, inferSchema=True)
➤ Average sales
python
df1.agg(fun.avg('sales'))
➤ Fill nulls
python
df1.fillna({'Name': 'Unknown', 'Sales': '400.5'})
✅ End of Task
This task showcases practical PySpark skills in data manipulation, aggregation, and cleaning. Perfect for portfolio, interviews, or academic submission.
