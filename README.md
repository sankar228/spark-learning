# spark-learning

## Databricks 

### Memory Sink ( "memory" ) 
Output modes: Append/Complete 
Fault Tolerance: No 
Delivery Guarantees: No 

### Console Sink ( "console" ) 
Output modes: Append/Update/Complete 
Fault Tolerance: No 
Delivery Guarantees: No 

## Streaming steps 
```
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Read data from source stream
inputDF = spark.readStream 
              .format("eventhub")
              .load()
    
# Apply transformation
resultDF = inputDF.where(...)
                  .withColumn(...)
                  .select(...)

# write stream to sink
#This is not a DF, this is call Streaming Query
streamingQuery = ( resultDF.writeStream
                      .queryName("nameofthequery")
                      .format("console")
                      .trigger(processingTime = '10 seconds')
                      .start()
                 )
                 

# check the content in the inputDF
## This will have actual data in 'body' column, and meta data columns from eventhub
display(
  inputDF,
  streamName = "DisplayMemoryQuery",
  processingTime = '10 seconds'
)

# create DF converting the binary input 'body' to string and assign a column name
startRawDF = (
                inputDF.withColumn(
                  "RawInputdata" , col("body").cast("strting")
                  )
                  .select("RawInputdata")
            )            
)

# Create Schema
dataSchema = (
                structType().add("id", "integer")
                  .add("name", "string")
)

# create a DF converting string json to table format by assigning the schema
startRawDF = (
                startRawDF
                  .select(
                          from_json(
                                col("RawInputdata"),
                                dataSchema
                          )
                          .alias("RawInputTable")
                  )
                  .select(
                      "RawInputTable.id",
                      "RawInputTable.name"
                  )
)


display(
  startRawDF,
  streamName = "DisplayMemoryQuery",
  processingTime = '10 seconds'
)


# Transformation

startTransDF = (
                startRawDF
                  .where("name == 'rod')
                  .withColumn("TripType",
                                  when(
                                    col("id") == 10, "SharedTrip"
                                  )
                                  .otherwise("SoloTrip") 
                   )
                   .drop("id")
)

## Start the stream
startConsoleQuery = (
              startTransDF.writeStream
                  .queryName("consolequery")
                  .format("console")
                  .trigger(processingTime = '10 sec')
                  .start()
)

```
 ## How to chain all the above operations to a single stream
 
 ```
 startTransDF = (
                  spark.readStream 
                    .format("eventhub")
                    .load()
                    
                   .withColumn(
                      "RawInputdata" , col("body").cast("strting")
                    )
                    
                    .select(
                          from_json(
                                col("RawInputdata"),
                                dataSchema
                          )
                          .alias("RawInputTable")
                     )
                     .select(
                        "RawInputTable.id",
                        "RawInputTable.name"
                     )
                     
                    .withColumn("TripType",
                                    when(
                                      col("id") == 10, "SharedTrip"
                                    )
                                    .otherwise("SoloTrip") 
                     )
                    .drop("id")
                    .where("name == 'rod')
 )
 
 startConsoleQuery = (
        startTransDF.writeStream
            .queryName("consolequery")
            .format("console")
            .trigger(processingTime = '10 sec')
            .start()
)
 
 ```

 
