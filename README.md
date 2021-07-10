# Spark 3.0.1-Scala-
使用Scala操作Spark

透過Spark使用分佈式數據集(RDD)經由SparkStream搭配滾動窗口或滑動窗口將即時數據進行處理分析,再存入數據庫(MySQL,redis,Kafak,HDFS)或直接輸出數據進行比對分析,
或是使用StructedStream整合Kafka經由WaterMark(Event Time)配合DSL或SparkSQL進行即時數據分析,再透過SparkML進行數據清理(ETL)與ML或ALS建立model,經由ALS即時輸出推薦項目,提供給使用者

- 1.用戶日誌分析(HanLP)
  - 透過 SougouLog網站後台的點擊資料搭配HanLP套件進行分詞,個別統計出"熱門搜索關鍵字統計","用戶熱門搜索詞","各個時間段搜索熱度統計"三種分析數據進行輸出分析
  
    RDD(數據輸入)=>.map()(包裝數據資料)=>.flatMap()(使用HanLP進行分詞)=>.reduceByKey()=>.sortBy()(排序)=>.take(10)(取出前10筆資料)=>數據分析輸出
    
- 2.及時數據處理
  - 將即時數據透過滑動動窗口進行數據分割,再取出前3排名,透過自定義輸出進行打印或存入數據庫(HDFSorMySQL)
  
    SparkStream(數據輸入)=>.reduceByKeyAndWindow()滑動窗口=>.transform()轉成底層RDD進行排序處理=>.take(3)(取出前3筆資料)=>數據分析自定義輸出(HDFS&MSQL)

- 3.電影評分數據分析

  - 對電影評分數據進行統計分析,獲取電影平均分TOP10,要求電影評分次數大於200
 
    SparkSession(數據輸入)=>Dataset轉DataFrame=>使用SparkSQL跟DSL進行數據分析排序處理(取出前10筆資料)=>數據分析輸出

- 4.電信基地台數據即時ETL(整合Kafka)

  - 透過Structured Streaming將Kafka數據源進行ETL,在將清理的數據輸出回Kafka
  
    Kafak stationTopic => Structured Streaming(ETL數據) => Kafka etlTopic

- 5.智能物聯網後台設備統計數據分析
    
  - 產生設備數據到Kafka,Structured Streaming即時統計消費數據,對網站設備訊號即時統計分析
  
    (1) 訊號強度大於30的設備
    (2) 各種設備類型的數量
    (3) 各種設備類型的平均訊號強度
    
    Kafak iotTopic => Structured Streaming(使用SparkSQL內痔函數解析Json數據) => 使用DSL跟SparkSQL數據分析 => 分析數據輸出

- 6.線上學習平台學習數據分析與即時推薦系統 

    (1) 即時分析學生答題狀況
    (2) 即時推薦易錯題給學生(SparkML-ALS推薦算法)
    (3) 離線分析學生學習狀況
    

                             StructuredStreaming即時分析
      學生學習數據(Kafka) =>                               => 即時分析推薦結果(存放MySQL/HBase) => SparkSQL離線分析
                              SparkStreaming即時推薦
                                     ||
                                     \/
                             Redis(存放ALS-model在HDFS的儲存路徑)  < = >   ALS推薦算法模型(存放在HDFS)
                           
- 7.預測
