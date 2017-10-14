package com.spark.dataengineertestapp.kafka;

public interface IProducer
{
    /**
     * Send playload to queue
     *
     * @param topic    : Topic in a queue
     * @param payload: Data to publish
     */
    void send(String topic, String payload);
}