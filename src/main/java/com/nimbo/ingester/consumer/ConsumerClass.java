package com.nimbo.ingester.consumer;



import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.ingester.writer.ParquetWriterService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerClass {
    private static IngestedRecord ingestedRecord;

    public static void consume(ParquetWriterService parquetWriter) throws Exception {

        Properties props = new Properties();

        props.put(

                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "10.10.61.54:9092"
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "netflow-java-consumer8"
        );

        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName()
        );

        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        // Load schema
        InputStream schemaStream =
                ConsumerClass.class
                        .getClassLoader()
                        .getResourceAsStream("avro/NetFlowV2Record.avsc");

        if (schemaStream == null) {
            throw new RuntimeException(
                    "NetFlowV2Record.avsc not found"
            );
        }

        Schema schema =
                new Schema.Parser().parse(schemaStream);

        GenericDatumReader<GenericRecord> reader =
                new GenericDatumReader<>(schema);

        // Output file
        Path outputPath = Path.of("netflow-output2.txt");

        try (
                KafkaConsumer<byte[], byte[]> consumer =
                        new KafkaConsumer<>(props);

                BufferedWriter writer =
                        Files.newBufferedWriter(outputPath)
        ) {

            consumer.subscribe(
                    Collections.singletonList("netflow")
            );

            while (true) {

                var records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<byte[], byte[]> record : records) {

                    try {

                        byte[] value = record.value();

                        BinaryDecoder decoder =
                                DecoderFactory
                                        .get()
                                        .binaryDecoder(
                                                value,
                                                null
                                        );

                        GenericRecord netflowRecord =
                                reader.read(null, decoder);

                        IngestedRecord ingestedRecord =
                                new IngestedRecord(
                                        netflowRecord,
                                        record.topic(),
                                        record.partition(),
                                        record.offset(),
                                        //serializedSizeBytes
                                        value.length

                                );

                        parquetWriter.write(ingestedRecord);

                        // Write entire record to file
//                        writer.write(netflowRecord.toString());
//                        writer.newLine();
//
//                        // Make sure data is written immediately
//                        writer.flush();

                    } catch (Exception e) {

                        writer.write(
                                "ERROR: " + e.getMessage()
                        );
                        writer.newLine();
                        writer.flush();
                    }
                }

                if (records.isEmpty()) {
                    System.out.println("empty");
                }


            }
        }
    }
}