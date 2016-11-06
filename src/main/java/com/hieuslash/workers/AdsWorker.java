package com.hieuslash.workers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import java.util.ArrayList;
import org.apache.avro.generic.IndexedRecord;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.common.errors.SerializationException;

public class AdsWorker extends Worker {

  public static void main(String[] args) {
    String topic = args[0];
    int numberOfConsumers = Integer.parseInt(args[1]);
    List<AdsWorker> consumers = new ArrayList<>();
    for (int i = 0; i < numberOfConsumers; i++) {
      AdsWorker consumer = new AdsWorker(topic);
      consumers.add(consumer);
      new Thread(consumer).start();
    }
  }

  public AdsWorker(String topic) {
    super(topic);
  }

  @Override
  public void doWork(ConsumerRecord<String, String> record) {
    System.out.println("Receive message: " + record.value() + ", Partition: "
        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
        + Thread.currentThread().getId());
  }

  @Override
  public void doWork(MessageAndMetadata record) {

    try {
      IndexedRecord key = (IndexedRecord) record.key();
      IndexedRecord value = (IndexedRecord) record.message();
      System.out.println(key);
      System.out.println(key);
      System.out.println(value);
    } catch(SerializationException e) {
      // may need to do something with it
    }
  }

}
