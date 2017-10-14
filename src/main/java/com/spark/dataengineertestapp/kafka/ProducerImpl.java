package com.spark.dataengineertestapp.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerImpl implements IProducer{

  private  String BOOTSTRAP_SERVERS = null;
  private Producer producer = null;
  private static final Logger logger = LoggerFactory.getLogger (ProducerImpl.class);
  
  public ProducerImpl(String bootstrap_server) {
    this.BOOTSTRAP_SERVERS = bootstrap_server;
    this.producer = createProducer();
  }
  private  Producer<Long,String> createProducer() {
    Properties props = new Properties();
    props.put("bootstrap.servers",BOOTSTRAP_SERVERS );
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return new KafkaProducer<>(props);
}

  @Override
  public void send (String topic, String payload) {
    long time = System.currentTimeMillis();
    final ProducerRecord<Long, String> record =
        new ProducerRecord<Long,String>(topic, time,
                    payload);

    
    try {
      Future<RecordMetadata> result = producer.send(record);
      final RecordMetadata contents = result.get();
      long elapsedTime = System.currentTimeMillis () - time;
      
      logger.info("sent record(key="+record.key()+" value="+record.value()+") " +
          "meta(partition="+contents.partition()+", offset="+contents.offset()+") time="+elapsedTime);
         
    }
    catch (Exception ex)
    {
      ex.printStackTrace ();
    }
   
  }
  
  
  
}
