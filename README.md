# 🚀 PySpark Task

This project demonstrates various PySpark operations using both RDDs and DataFrames. It covers data creation, transformation, aggregation, filtering, and text processing.

---

## 🔧 Requirements

- Python 3.x  
- PySpark  
- NumPy  
- SparkSession initialized

---

## 🧮 RDD Operations

### ➤ Create RDD from numbers 1 to 49
```python
RDD = sc.parallelize(np.arange(1,50))

➤ Basic statistics
print("Sum =", RDD.sum())
print("Average =", RDD.mean())
print("Max =", RDD.max())
print("Min =", RDD.min())
print("Count =", RDD.count())

➤ Count even vs. odd numbers
Odd = RDD.filter(lambda x: x % 2 != 0)
Even = RDD.filter(lambda x: x % 2 == 0)
print("ODD Numbers:", Odd.count(), "EVEN Numbers:", Even.count())

👥 People Data RDD
people_data = [("Nada", 25), ("Mona", 30), ("Ahmed", 35), ("Khaled", 40), ("Ahmed", 35), ("Nada", 25)]
rdd_people = sc.parallelize(people_data)

➤ Oldest person
rdd_people.max(key=lambda x: x[1])

➤ Average age
rdd_people.map(lambda x: x[1]).mean()

➤ Group names by age
rdd_people.groupBy(lambda x: x[1]).map(lambda x: (x[0], list(x[1]))).collect()

## 📄 Text RDD Analysis
➤ Load text file
rdd_russia = sc.textFile("/data/russia.txt")

➤ Line count
rdd_russia.count()

➤ Lines containing "Russia"
rdd_russia.filter(lambda line: "Russia" in line).count()

➤ Top 5 frequent words
rdd_russia.flatMap(lambda line: line.split())
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a + b)
          .sortBy(lambda x: x[1], ascending=False)
          .take(5)

➤ Tokenization and stopword removal
stopwords = ['a', 'the', 'is', 'to', 'in', 'of']
rdd_russia.flatMap(lambda line: line.split())
          .filter(lambda word: word not in stopwords)
