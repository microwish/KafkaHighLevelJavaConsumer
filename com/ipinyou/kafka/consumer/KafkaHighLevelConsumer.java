package com.ipinyou.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KafkaHighLevelConsumer {
    private final ConsumerConnector consumerConnector;
    private final String topic;
    private ExecutorService executor;

    public KafkaHighLevelConsumer(String zkServers, String groupId,
                                  String topic)
    {
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(zkServers, groupId));
        this.topic = topic;
    }

    public void shutdown()
    {
        if (consumerConnector != null) consumerConnector.shutdown();
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    System.out.println("Timed out waiting for consumer threads"
                            + " to shut down, exiting uncleanly");
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted during shutdown,"
                        + " exiting uncleanly");
            }
        }
    }

    public void run(int numThreads, RunningConsumer[] consumers)
        throws Exception
    {
        if (numThreads != consumers.length) {
            throw new Exception("number of consumers wrong");
        }

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(numThreads));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(numThreads);

        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            RunningConsumer consumer = consumers[threadNumber];
            consumer.setStream(stream);
            consumer.setThreadNumber(threadNumber);
            executor.submit(consumer);
            threadNumber++;
        }
    }

    protected static ConsumerConfig createConsumerConfig(String zkServers,
                                                         String groupId)
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkServers);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }
}
