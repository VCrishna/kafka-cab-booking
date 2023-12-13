package com.ProducerDemo.Producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Create Producer properties
        String bootstrapServer = "localhost:9092";
        Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", bootstrapServer);
//		properties.setProperty("key.serializer", StringSerializer.class.getName());
//		properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello world  "+Integer.toString(i);
            String key = "id_"+Integer.toString(i);

            // IMPORTANT
            // Same key always goes to same partition
            logger.info("\nKey: "+key);

            // Create Producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic,key,value);

            // send data - asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if(e == null) {
                        // record was successfully sent
                        logger.info("\nReceived new Metadata: \n" +
                                "Topic --> "+recordMetadata.topic()+
                                "\nPartition  --> "+recordMetadata.partition()+
                                "\nOffset -->  "+recordMetadata.offset()+
                                "\nTimestamp --> "+recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Exception occured while producing ",e);
                    }

                }
            }).get(); // with .get() --> blocked the .send() to make it synchronous -- don't do it in production!
        }

        // flush data
        kafkaProducer.flush();

        // flush and close producer
        kafkaProducer.close();
    }
}
