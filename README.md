# spark_note

## 1. spark core  ##
  * anconda套件庫路徑: anaconda3/lib/python3/site-packages
  * 如果是使用spark-submit 執行py檔，需要在檔案內定義`sc = SparkContext()`
  * 建立RDD物件
    ```js
    #創建list的方法
    data = [1,2,3,4,5]
    datardd = sc.parallelize(data)
    #或是開檔
    rdd = sc.textFiles(hdfs_path)
    ```
  * rdd物件操作:
    ```js
    rdd.map(f or lambda)
    rdd.flatmap(f or lambda)
    rdd.filter(func) #func必須return bool
    rdd.distinct()
    rdd.sortBy(f or lambda)
    rdd.map(f)
    rdd.reduce(f)
    rdd.mapValues(f)
    rdd.reduceByKey()
    rdd.groupByKey() # Output: [(1,(iterable)),(2,(iterable))]
    rdd = sc.union([rdd1,rdd2]) #combine
    #查看
    rdd.collect()
    rdd.take(num)
    rdd.first()
    rdd.count()
    ###################CombineByKey#################################
    In [120]: x = sc.parallelize([('B',1),('B',2),('A',3),('A',4),('A',5)])
     ...: createCombiner = (lambda el: [(el, el**2)])
     ...:
     ...: mergeVal = (lambda aggregated, el : aggregated + [(el, el**2)])
     ...: mergeComb = (lambda agg1, agg2 : agg1 + agg2)
     ...:
     ...: y = x.combineByKey(createCombiner, mergeVal, mergeComb)
     ...: print(x.collect())
     ...: print(y.collect())
     [('B', 1), ('B', 2), ('A', 3), ('A', 4), ('A', 5)]
     [('A', [(5, 25), (4, 16), (3, 9)]), ('B', [(1, 1), (2, 4)])]
     關於combinebykey: https://www.linkedin.com/pulse/spark-pyspark-combinebykey-simplified-swadeep-mishra-1/
    #######################################################################

    #join
    rdd1.join(rdd2)
    rdd1.leftOuterJoin(rdd2)
    rdd1.rightOuterJoin(rdd2)
    rdd1.fullOuterJoin(rdd2)
    #cache
    rdd.persist()
    ```
  * 計數器:[用法範例](https://github.com/a13140120a/Spark_Note/blob/master/proj_Spark_Core/avg_temperature3.py)
    ```js
    In [94]: accum = sc.accumulator(0) #必須放在func內

    In [95]: sc.parallelize([1,2,3,4]).foreach(lambda x: accum.add(x)) #foreach:各自執行
    In [96]: accum.value
    Out[96]: 10
    ```
  * 存到本地端:
    ```js
    rdd.saveAsTextFile("file:///path1/path2")
    ```
  * 夾帶檔案, [example](https://github.com/a13140120a/Spark_Note/blob/master/proj_spark_sql/create_udf_page_views_df.py)
    ```JS
    pyspark --master spark://master:7077 --py-files /PATH/xxx.zip  #夾帶多個py檔(使用module時)打包成zip檔\
    #註:多個zip檔用逗號隔開
    #打包zip
    zip -r /home/folder/target.zip /home/folder2/subfolder/*

    pyspark --master spark://master:7077 --py-files /PATH/xxx.py  #夾單個py檔

    pyspark --master spark://master:7077 --files /PATH/file1  #夾帶其他檔案

    pyspark --master spark://master:7077 --jars  /PATH/file
    # 用pyspark讀取檔案路徑時只要讀取當前目錄就可以了
    ```

## 2. spark SQL  ##

  * 功能多寡: Spark SQL > Hive SQL > SQL
  * spark SQL table的一個row 就是一個rdd
  * 檔案類型:
    1. Row-Based: Csv, Json, Avro, Sequencefile
      一次要讀取一整個ROW
    2. Column-Based: Parquetfile, ROC, RC
      可讀取指定欄位(佔記憶體空間較小，大型資料較常用)

  * jdbc連線MySQL(version8以上):, [mySQL安裝](https://github.com/a13140120a/SQL_note/blob/master/README.md)
    * 下載官網JDK檔(官網-> connector/j -> platform independent)

    * 修改 /etc/mysql/mysql.conf.d/mysqld.cnf 檔:
      ```js
      bind-address          = 127.0.0.1
                           (改成要接受連線的ip，如果想要允許任何人連線就註解掉)
      ```
    * 重啟
      ```js
      sudo /etc/init.d/mysql restart
      ```
    * 登入:
      ```js
      mysql -u root -p
      ```
    * 新增user:
      ```js
      CREATE USER 'newuser'@'%' IDENTIFIED BY 'mypasswd';
      "%"代表任何ip都可以登入,IDENTIFIED BY [密碼]

      # 將所有 database 下的 table 都給予 newuser 所有權限
      GRANT ALL ON *.* TO 'newuser'@'%';

      #查詢user:
      select user, host from mysql.user;

      #刪除user
      DROP USER 'newuser'@'%';
      ```
    * 遇到問題:Your password does not satisfy the current policy requirements
      代表密碼太短，解決:
      ```js
      MySQL內部
      set global validate_password.length=1;
      set global validate_password.policy=0;

      #查詢設定:
      SHOW VARIABLES LIKE 'validate_password%';
      ```
     * 啟動pyspark:
       ```js
       pyspark --master spark://master:7077 --jars "JDBC檔"(或是放到每台的spark資料夾底下的jars資料夾內)(或是設定每台的spark-env.sh)
       ```
     * 寫入mysql:[示範檔](https://github.com/a13140120a/Spark_Note/blob/master/proj_spark_sql/crime_data_stats_wirte_to_mysql.py)
     * 讀取mysql:
     ```js
     prop = {'user': 'user',
            'password': '1234',
            'driver': 'com.mysql.cj.jdbc.Driver'}

     url = 'jdbc:mysql://host:port(預設3306)/name'

     df = spark.read.jdbc(url=url, table='table_name', properties=prop)

     df.show()
     ```

  * Create SparkSession:
    ```js
    spark = SparkSession \
      .builder \
      .getOrCreate()
    ```
  * 基本操作:
    * create dataframe
      ```JS
      df = spark.read.csv("hdfs://PATH/data.csv",
                        header=True, #有無欄位
                        inferSchema=True) #自動分配資料型態
      ###########
      官網參數 : https://spark.apache.org/docs/2.2.0/api/python/pyspark.sql.html
      ############

      value = [("AAPL", "Apple"), ("CSCO","Cisco Systems")]

      #自定義Schema
      #[spark.createDataFrame(value, "column1: type, column2: type")]
      df = spark.createDataFrame(value, "symbol: string, names: string")

      ```

    * select語法
      ```JS
      df.createOrReplaceTempView("viewtable") #要先設定View table
      result = spark.sql("SQL語法 FROM viewtable")
      df["column"].desc()  # 降冪
      df.orderBy(df["column"].desc())
      df["column"] +10
      df.select(df["column"], df["column2"] +10)

      from pyspark.sql.functions import format_number
      df.select(format_number(df["column"],1))  #取到小數點後第一位
      ```
    * na處理
      ```js
      df.dropna(how="any",subset=["column1","column2"]) #其中一個有na就刪掉
      df.dropna(how="all",subset=["column1","column2"]) #兩個都有na才刪掉
      df.fillna("fillthing",subset = ["column1"]).fillna("fillthing",subset = ["column2"])  #fill兩個欄位要分開寫
      ```
    * filter(等同where)
      ```js
      df.filter("year >= 2015") # 用字串的方法表示
      df.filter(df["column"] == "something").show()
      df.filter((df["column1"] <= 200) & ~(df["column2"] > 30)).show() # 波浪符 = not
      df.filter("close <= 200 and not(open > 30)").show()              # 字串方法表示
      df.where(df["column"] == "something").show()    # where的方法
      ```
    * drop欄位
      ```js
      df.drop("column")
      ```
    * 更改欄位
      ```js
      df.withColumnRenamed("column", "new_column")   # 修改欄位名稱
      df.agg({"column_name": "sum"}).collect()[0][0] # collect回傳list物件內容包含row物件，再取物件的第一個欄位值
      convictions_by_borough_with_percentage = convictions_by_borough.withColumn("new_column",function) #function: sum, avg etc.....

      # 新增欄位
      df.select("*", (df["int_type_column1"] - df["int_type_column2"]).alias("new_column")).show()
      df.withColumn("new column", df["int_type_column1"] - df["int_type_column2"]).show()
      ```

    * GroupBy
      ```js
      df.groupBy("column1").agg({"value": "func"}) # func: avg, max, min, sum, count.

      #搭配多個agg使用
      df.groupBy(df["column"]).agg(avg("column2"), stddev("column3"), max("column4"), min("column5"), sum("column6")).show()

      #一個function所有欄位
      df.groupBy(df["column"]).mean().show()

      #計算種類
      df.agg(countDistinct("column")).show()

      ```
    * limit
      ```js
      df2 = df.limit(10)
      df2.show()
      ```

    * join
      ```
      df1.join(df2,["join_column","right"])
      #how :type(str),
      default inner.
      Must be one of: inner, cross, outer, full, full_outer, left, left_outer, right, right_outer, left_semi, and left_anti.

      df1.join(df2, df1["column"] == df2["column"],"right").show() # 顯示df2所有資料，若df2有 df1沒有則產生null
      df1.join(df2, df1["column"] == df2["column"]).show()  # 只顯示df1有的資料，不會產生null

      ```

    * 查看表格
      ```js
      df.show(n)       # 查看前n筆
      df.printSchema() # 查看資料型態
      ```
    * 轉成pandas
      ```js
      df.toPandas()
      ```

    * 轉成rdd (內部元素會變成row物件)
      ```js
      rdd = df.rdd
      rddlist = rdd.collect()
      #rddlist=[row,row,row,row]
      ```

    * 操作row物件
      ```js
      row = Row(column="value",column2="value2",column3="value3",) #create row
      row["column"]        # 查看columu的value
      row.column           # 同上
      row["column"].desc() # 降冪排列
      row[0]               # 查看第一個欄位
      temp = ddd.map(lambda row : (row["column1"],row["column2"]))
      ```

    * RDD_to_df
      ```js
      In [32]: lines = sc.parallelize([("Michel",29),("Andy",30),("Justin",19)])
      In [33]: lines.collect()
      Out[33]: [('Michel', 29), ('Andy', 30), ('Justin', 19)]
      In [34]: schema = "name string,age int"   #自定義schema : "column type"
      In [35]: schemalines = spark.createDataFrame(lines,schema)
      In [36]: schemalines.show()
      +------+---+
      |  name|age|
      +------+---+
      |Michel| 29|
      |  Andy| 30|
      |Justin| 19|
      +------+---+
      ```

    * 存檔
      ```js
      df.write.parquet("hdfs:/path/parquet")
      result_df.write.json("hdfs://PATH/json1")
      ```
  ----
  * Afinn(輿情分析):
    ```js
    pip install Afinn
    from afinn import Afinn
    model = Afinn()
    model.score("this is a sentence")

    def score_message_py(msg):
        global model
        return model.score(msg)
    ```

  * 註冊function(UDF):
    ```js
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import * #很多SQL 的module
    from pyspark.sql.types import *

    def slen_py(s):
        return len(s)

    spark.udf.register("slen", slen_py, IntegerType())   #for SQL
    slen = udf(slen_py, IntegerType())                   #for DataFrame transformation Api
    #array type要設定ArrayType(SomeType())

    #Use For SQL
    df.createOrReplaceTempView("stocks")
    spark.sql("select slen(column) as alias_of_column from table").show()

    #Use for DataFrame transformation Api
    df.select(slen("column").alias("alias_of_column")).show()

    ```

## 3. Spark MLlib ##

    * ALS演算法延伸閱讀:
      * [als-recommender-pyspark](https://github.com/snehalnair/als-recommender-pyspark)
      * [PySpark Collaborative Filtering with ALS](PySpark Collaborative Filtering with ALS)
      * [ALS演算法實現使用者音樂打分預測](https://www.mdeditor.tw/pl/2jyv/zh-tw)
      * [Pyspark官方文件](https://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#pyspark.mllib.recommendation.ALS)













