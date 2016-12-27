package com.hieuslash.workers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.List;
import java.util.ArrayList;
import org.apache.avro.generic.IndexedRecord;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.common.errors.SerializationException;
import com.hieuslash.db.ESClient;
import com.hieuslash.Utils;
import kafka.consumer.KafkaStream;
import java.util.stream.StreamSupport;
import java.util.Spliterators;
import java.util.Spliterator;

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
  public void doWork(KafkaStream kafkaStream) {
    StreamSupport.stream(Spliterators.spliteratorUnknownSize(kafkaStream.iterator(), Spliterator.ORDERED), false)
    .map(record -> {
      MessageAndMetadata metaRecord = (MessageAndMetadata) record;
      return metaRecord.message();
    })
    .forEach(message -> System.out.println(message));
  }

  @Override
  public void doWork(ConsumerRecord<String, String> record) {
    System.out.println("Receive message: " + record.value() + ", Partition: "
        + record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
        + Thread.currentThread().getId());
  }

  @Override
  public void doWork(MessageAndMetadata record) {
    ESClient esClient = new ESClient();
    try {
      IndexedRecord key = (IndexedRecord) record.key();
      IndexedRecord value = (IndexedRecord) record.message();
      System.out.println(Utils.jsonMap(key.toString()).toString());
      System.out.println(value);
      //Elastic Job
      esClient.index(Utils.jsonMap(value.toString()));
    } catch(SerializationException e) {
      // may need to do something with it
    }
  }

}
