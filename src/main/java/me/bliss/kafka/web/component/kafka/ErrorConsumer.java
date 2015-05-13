package me.bliss.kafka.web.component.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.kafka, v 0.1 5/13/15
 *          Exp $
 */
public class ErrorConsumer {

    private ConsumerConnector consumerConnector;

    public ErrorConsumer(){
        Properties props = new Properties();
        props.put("zookeeper.connect", "zassets.ui.alipay.net:2181");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("group.id", "9527");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "100");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        consumerConnector = Consumer
                .createJavaConsumerConnector(new ConsumerConfig(props));
    }

    public void listenErrorMessage(){
        new Thread(new Runnable() {
            @Override public void run() {
                System.out.println("准备接收error消息");
                HashMap<String, Integer> topicMap = new HashMap<String, Integer>();
                topicMap.put("qt_error", 1);
                StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
                StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
                Map<String, List<KafkaStream<String, String>>> messageStreams = consumerConnector.createMessageStreams(topicMap, keyDecoder, valueDecoder);
                final List<KafkaStream<String, String>> kafkaStreams = messageStreams.get("qt_error");
                for (KafkaStream stream : kafkaStreams){
                    final ConsumerIterator consumerIterator = stream.iterator();
                    while(consumerIterator.hasNext()){
                        String buildMessage = consumerIterator.next().message().toString();
                        System.out.println("Error监听器引擎收到的消息是: " + buildMessage);
                    }
                }
            }
        }).start();

    }
}
