package com.hieuslash.workers;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Arrays;
import java.lang.InterruptedException;
import org.apache.avro.generic.IndexedRecord;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import java.util.Map;
import java.util.List;
import java.util.HashMap;

import com.hieuslash.WorkerBoot;
import com.hieuslash.WorkerConfig;

@WorkerBoot
public abstract class Worker implements Runnable{

  private final String topic;
  private final ConsumerConnector consumer;
  private Properties consConfig;

  public Worker(String topic) {
    this.topic = topic;
    consumer = createConnector();
  }

  private ConsumerConnector createConnector()
  {
    consConfig = WorkerConfig.getConsumerConfig(Worker.class);
    return Consumer.createJavaConsumerConnector(new ConsumerConfig(consConfig));
  }

  private KafkaStream createStream()
  {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(this.topic, new Integer(1));
    VerifiableProperties verifiableConsConfig = new VerifiableProperties(consConfig);
    KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(verifiableConsConfig);
    KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(verifiableConsConfig);
    Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
                                                                            topicCountMap,
                                                                            keyDecoder,
                                                                            valueDecoder
                                                                          );
    return consumerMap.get(topic).get(0);
  }

  @Override
  public void run() {
    KafkaStream stream = createStream();
    doWork(stream);
    // ConsumerIterator it = stream.iterator();
    // while (it.hasNext()) {
    //   try {
    //     MessageAndMetadata messageAndMetadata = it.next();
    //     doWork(messageAndMetadata);
    //     if(Thread.currentThread().isInterrupted())
    //     {
    //       throw new InterruptedException();
    //     }
    //   } catch (InterruptedException e) {
    //       System.out.println("stop running thread");
    //       Thread.currentThread().interrupt();
    //   }
    // }
  }
  abstract void doWork(KafkaStream stream);
  abstract void doWork(ConsumerRecord<String, String> record);
  abstract void doWork(MessageAndMetadata record);

}
