package com.github.projectsval;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemoWithCallback {
    public static void main( String[] args )
    {

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //serializes string into bytes
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("valentin_topic", "hello world!");

        //Produce/send data asynchronously
        producer.send(record, new Callback() {
            public void OnCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is created
            }
        });

        //wait for the data to be produced
        producer.flush();
        producer.close(); //flush and close
    }
}
