package com.ProducerDemo.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        try {
            new ConsumerDemoWithThreads().run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    private void run() throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-application";
        String topic = "first_topic";

        // Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the Consumer Thread !");

        // Create the Consumer Runnable
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );
        // Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            logger.info("Caught Shutdown Hook!");
                            myConsumerRunnable.shutdown();
                            try {
                                latch.await();
                            }
                            catch (InterruptedException e) {
                                logger.error("Error ",e);
                            }
                            logger.info("Application has exited");
                        }
                )
        );

        try {
            latch.await();
        }
        catch (InterruptedException e) {
            logger.error("Application got Interrupted",e);
        }
        finally {
            logger.info("Application is closing");
        }
    }
    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch countDownLatch;
        // create Consumer
        KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerRunnable(String bootstrapServers,String topic,String groupId,CountDownLatch latch) {
            this.countDownLatch = latch;
            // Create Properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            this.kafkaConsumer = new KafkaConsumer<>(properties);
            // Subscribe Consumer to our Topic(s)
            // for only 1 topic
            kafkaConsumer.subscribe(Collections.singleton(topic));
            // for list of topics
            // kafkaConsumer.subscribe(Arrays.asList("first_topic","second_topic"));
        }
        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> consumerRecord = kafkaConsumer.poll(Duration.ofMillis(10000));
                    for (ConsumerRecord<String, String> record: consumerRecord) {
                        logger.info("Key: "+record.key() +", Value"+record.value());
                        logger.info("Partition: "+record.partition()+", Offset: "+record.offset());
                    }
                }
            }
            catch (WakeupException e) {
                logger.info("Received Shutdown Signal!");
            }
            finally {
                kafkaConsumer.close();
                // tell our main code we're done with the consumer
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup( method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            kafkaConsumer.wakeup();
        }
    }
}
