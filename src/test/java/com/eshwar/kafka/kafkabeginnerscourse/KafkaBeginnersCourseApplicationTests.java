package com.eshwar.kafka.kafkabeginnerscourse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaBeginnersCourseApplicationTests {

    Logger logger = LoggerFactory.getLogger(KafkaBeginnersCourseApplicationTests.class);

    @Test
    public void contextLoads() {

        System.out.println("hellow world");

        String bootstrapServer = "127.0.0.1:9092";
        // Create the properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        //Create the producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello dude");

        //send data

        producer.send(record);

        producer.flush();

        producer.close();

    }

    @Test
    public void test() {


        String bootstrapServer = "127.0.0.1:9092";
        // Create the properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            //Create the producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello Man" + i);

            //send data

            producer.send(record, ((metadata, exception) -> {

                if (exception == null) {
                    logger.info(" Received new metadata " + "\n" + "topic : " + metadata.topic() + "\n"
                            + "partition : " + metadata.partition() + "\n"
                            + "offset : " + metadata.offset() + "\n"
                            + "timestamp : " + metadata.timestamp());

                } else {
                    logger.error("exception occured ", exception);
                }
            }));

        }

        producer.flush();

        producer.close();
    }

    @Test
    public void testWithKeys() throws ExecutionException, InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";
        // Create the properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);


        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "hello Apple" + i;
            String key = "id " + i;
            //Create the producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            //send data

            logger.info("key : " + key);
            producer.send(record, ((metadata, exception) -> {

                if (exception == null) {
                    logger.info(" Received new metadata " + "\n" + "topic : " + metadata.topic() + "\n"
                            + "partition : " + metadata.partition() + "\n"
                            + "offset : " + metadata.offset() + "\n"
                            + "timestamp : " + metadata.timestamp());

                } else {
                    logger.error("exception occured ", exception);
                }
            })).get(); // blocks the call, dont use this in production

        }

        producer.flush();

        producer.close();

    }


    @Test
    public void testConsumer() {

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        while (true) {

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord<String, String> record : consumerRecords) {

                logger.info("Key : " + record.key() + " Value : " + record.value());
                logger.info("Partition : " + record.partition() + " Offset :" + record.offset());

            }
        }


    }


    @Test
    public void consumerThread() {


        class ConsumerRunnable implements Runnable {

            KafkaConsumer<String, String> kafkaConsumer;
            CountDownLatch latch;

            ConsumerRunnable(CountDownLatch latch, String bootstrapServer, String groupId, String topic) {

                Properties properties = new Properties();
                properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
                properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
                properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                this.kafkaConsumer = new KafkaConsumer<String, String>(properties);
                kafkaConsumer.subscribe(Arrays.asList(topic));
                this.latch = latch;

            }


            @Override
            public void run() {

                try {
                    while (true) {

                        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

                        for (ConsumerRecord<String, String> record : consumerRecords) {

                            logger.info("Key : " + record.key() + " Value : " + record.value());
                            logger.info("Partition : " + record.partition() + " Offset :" + record.offset());

                        }
                    }

                } catch (WakeupException e) {
                    logger.info("recieved shutdown signal");
                } finally {
                    kafkaConsumer.close();
                    latch.countDown(); // tell main code to ext
                }
            }

            public void shutDown() {

                //wake up method is special method to interrupt consumner.poll
                // it will throw wakeup exception
                kafkaConsumer.wakeup();
            }
        }


        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("creating the consumer");
        ConsumerRunnable runnable = new ConsumerRunnable(latch, bootstrapServer, groupId, topic);

        //start thread
        Thread thread = new Thread(runnable);
        thread.start();

        //add a shutdownhook

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught sutdown hook");
            runnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();

        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application closing");
        }


    }

    @Test
    public void assignAndSeek() {

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-seventh-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = 15L;
        kafkaConsumer.assign(Arrays.asList(topicPartition));
        kafkaConsumer.seek(topicPartition, offset);
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;


        while (keepOnReading) {

            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100)); //new in kafka 2.0.0

            for (ConsumerRecord<String, String> record : consumerRecords) {
                numberOfMessagesReadSoFar = numberOfMessagesToRead + 1;
                logger.info("Key : " + record.key() + " Value : " + record.value());
                logger.info("Partition : " + record.partition() + " Offset :" + record.offset());

                if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }


    }

}
