# big_data_final_proj, steps


## dataset found at
https://smoosavi.org/datasets/lstw

## create local mongoDB instance with 
```pip install pymongo

cd ~/mongodb
rm -rf *  # Clear current files
wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2204-7.0.4.tgz

tar -xzf mongodb-linux-x86_64-ubuntu2204-7.0.4.tgz
mv mongodb-linux-x86_64-ubuntu2204-7.0.4/* .
rm mongodb-linux-x86_64-ubuntu2204-7.0.4.tgz
rmdir mongodb-linux-x86_64-ubuntu2204-7.0.4

~/mongodb/bin/mongod --dbpath ~/mongodb-data --logpath ~/mongodb-logs/mongo.log --port 27017 --fork
```


## instructions in pipelineTotal.py, run in nyu dataproc with 

```spark-submit     --master yarn     --deploy-mode client     --driver-memory 8g     --driver-cores 4     --executor-memory 8g     --executor-cores 8     --num-executors 10     --conf spark.driver.maxResultSize=4g     --conf spark.sql.adaptive.enabled=true     --conf spark.sql.adaptive.coalescePartitions.enabled=true     --conf spark.sql.adaptive.skewJoin.enabled=true     --conf spark.sql.shuffle.partitions=400     --conf spark.default.parallelism=400     --conf spark.serializer=org.apache.spark.serializer.KryoSerializer     --conf spark.kryoserializer.buffer.max=1024m     --conf spark.dynamicAllocation.enabled=false     --conf spark.memory.fraction=0.8     --conf spark.memory.storageFraction=0.3     --conf spark.sql.execution.arrow.pyspark.enabled=true     --conf spark.sql.execution.arrow.maxRecordsPerBatch=20000     --conf spark.network.timeout=800s     --conf spark.executor.heartbeatInterval=60s   pipelineTotal.py```


## access mongoDB with 

```python viewMongo.py <table_name>```