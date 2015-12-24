package com.ipinyou.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class RunningConsumer implements Runnable {
    protected KafkaStream stream;
    protected int threadNumber;

    public RunningConsumer(KafkaStream stream, int threadNumber)
    {
        this.stream = stream;
        this.threadNumber = threadNumber;
    }

    public RunningConsumer(KafkaStream stream)
    {
        this.stream = stream;
        this.threadNumber = -1;
    }

    public RunningConsumer(int threadNumber)
    {
        this.stream = null;
        this.threadNumber = threadNumber;
    }

    public RunningConsumer()
    {
        this.stream = null;
        this.threadNumber = -1;
    }

    public void setStream(KafkaStream stream)
    {
        this.stream = stream;
    }

    public void setThreadNumber(int threadNumber)
    {
        this.threadNumber = threadNumber;
    }

    public void run()
    {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Thread " + threadNumber + ": "
                    + new String(it.next().message()));
        }
        System.out.println("Shutting down Thread: " + threadNumber);
    }
}
