package com.hazelcast.jet.demo.cdc;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.server.JetBootstrap;
import org.apache.kafka.connect.json.JsonDeserializer;

import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * Simple Jet pipeline which consumes CDC events originated from MySQL via Debezium
 * from Kafka, prints the objects to the standard output in the string format and
 * writes them to a Hazelcast IMap..
 */
public class CDCWithJet {

    public static void main(String[] args) {
        JetInstance jet = JetBootstrap.getInstance();

        Properties properties = new Properties();
        properties.setProperty("group.id", "cdc-demo");
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("key.deserializer", JsonDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", JsonDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        Pipeline p = Pipeline.create();

        p.drawFrom(KafkaSources.kafka(properties, record -> {
            JsonValue key = Json.parse(record.key().toString());
            JsonValue value = Json.parse(record.value().toString());
            return tuple2(key, value);
        }, "dbserver1.inventory.customers"))
         .withoutTimestamps()
         .peek()
         .drainTo(Sinks.map("customers"));

        jet.newJob(p).join();
    }
}
