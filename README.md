# Spark-Scala-
使用Scala操作Spark

透過Spark使用分佈式數據集(RDD)經由SparkStream搭配DSL或SparkSQL將數據庫(MySQL,redis,Kafak,K8s)或大數據資料庫(Hive,HDFS,HBase),搭配SparkML進行數據資料清洗與建模,分析數據資料輸出分析數據,或是使用StructedStream整合Kafka搭配WaterMark進行即時數據分析,再透過SparkML進行數據清理(ETL)與ALS建立model,經由ALS即時輸出推薦項目,提供給使用者分析

- 1.用戶日誌分析(HanLP)
  - 透過 SougouLog網站後台的點擊資料搭配HanLP套件進行分詞,個別統計出"熱門搜索關鍵字統計","用戶熱門搜索詞","各個時間段搜索熱度統計"三種分析數據進行輸出分析
  
    RDD(數據輸入)=>.map()(包裝數據資料)=>.flatMap()(使用HanLP進行分詞)=>.reduceByKey()=>.sortBy()(排序)=>.take(10)(取出前10筆資料)=>數據分析輸出
