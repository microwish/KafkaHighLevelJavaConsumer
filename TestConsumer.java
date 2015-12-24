package test;

import com.ipinyou.kafka.consumer.*;
import kafka.consumer.KafkaStream;

class MyConsumer extends RunningConsumer {
    public MyConsumer(KafkaStream stream, int threadNumber)
    {
        super(stream, threadNumber);
    }

    public MyConsumer(KafkaStream stream)
    {
        super(stream);
    }

    public MyConsumer(int threadNumber)
    {
        super(threadNumber);
    }

    public MyConsumer()
    {
        super();
    }

    public void run()
    {
        super.run();
    }
}

class ConsumerExample {
    public static void main(String[] args)
    {
        String zkServers = args[0];
        String groupId = args[1];
        String topic = args[2];
        int partitionTotal = Integer.parseInt(args[3]);
        MyConsumer[] workers = new MyConsumer[partitionTotal];
        for (int i = 0; i < partitionTotal; i++) {
            workers[i] = new MyConsumer();
        }

        KafkaHighLevelConsumer consumer =
            new KafkaHighLevelConsumer(zkServers, groupId, topic);
        try {
            consumer.run(partitionTotal, workers);
        } catch (Exception e) {
            System.out.println(e);
            consumer.shutdown();
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
        }
        consumer.shutdown();
    }
}
