/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shapira.examples.producer.simplecounter;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DemoProducerNewJava implements DemoProducer {

    String topic;
    String sync;
    private Properties kafkaProps = new Properties();
    private Producer<String, String> producer;

    public DemoProducerNewJava(String topic) {
        this.topic = topic;
    }

    @Override
    public void configure(String brokerList, String sync) {
        this.sync = sync;
        kafkaProps.put("bootstrap.servers", brokerList);

        // This is mandatory, even though we don't send keys
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("acks", "1");

        // how many times to retry when produce request fails?
        kafkaProps.put("retries", "3");
        kafkaProps.put("linger.ms", 5);
    }

    @Override
    public void start() {
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    @Override
    public void produce(String value) throws ExecutionException, InterruptedException {
        if (sync.equals("sync"))
            produceSync(value);
        else if (sync.equals("async"))
            produceAsync(value);
        else throw new IllegalArgumentException("Expected sync or async, got " + sync);

    }

    @Override
    public void close() {
        producer.close();
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void produceSync(String value) throws ExecutionException, InterruptedException {
        /*
        for ProducerRecord, actually we can specify not only which topic, but also parition, message key, message value
        the complete version could be : record = new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1)
        */
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

        /*
        see info below
        */
        producer.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void produceAsync(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);

        /*
        for send,
        1. regarding callback -
        acctually the callback is a way to compensate for what's included in the sync mode, but not in async mode.
        in sync mode, the server is forced to respond for each message acknowledgement; however,
        in async mode, you need custom code to handle what's the next, because async send will return immediately after the message is buffered successfully.
        2. regarding the result of send -
        The result of the send is a RecordMetadata specifying the partition the ‘record’ was sent to and the ‘offset’ it was assigned.
        */
        producer.send(record, new DemoProducerCallback());
    }

    private class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            }
        }
    }
}
