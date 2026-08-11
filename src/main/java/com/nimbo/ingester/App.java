package com.nimbo.ingester;

import com.nimbo.ingester.consumer.ConsumerClass;
import com.nimbo.ingester.writer.ParquetWriterService;

public class App {

    public static void main(String[] args) throws Exception {

        ParquetWriterService parquetWriter =
                new ParquetWriterService(
                        "hdfs://10.10.61.52:9000",
                        "/data/netflow/netflow.parquet"
                );
        ConsumerClass consumer =
                new ConsumerClass();

        consumer.consume(
                parquetWriter
        );
    }
}



