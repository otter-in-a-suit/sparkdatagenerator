## Spark data generator
Spark example for my blog article "Update an HBase table with Hive... or sed": https://otter-in-a-suit.com/blog/?p=12

# Build
`mvn clean install`

# Run
`cd target
spark-submit \
--class com.otterinasuit.spark.datagenerator.Main \
--master localhost:9000 \
--deploy-mode client \
CreateTestHBaseRecords-1.0-SNAPSHOT.jar \
2000000 \ # number of records
4 # threads`
