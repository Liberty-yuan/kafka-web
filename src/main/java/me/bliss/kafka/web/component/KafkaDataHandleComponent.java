package me.bliss.kafka.web.component;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component, v 0.1 3/26/15
 *          Exp $
 */
public class KafkaDataHandleComponent {

    private String clientName = "lanjue_kafka_"+new Date().getTime();

    public long getEarliestOffset(String brokerHost,int port, String topic,int partition) {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try{
            simpleConsumer = new SimpleConsumer(brokerHost, port, 100000,
                    1024 * 1024, clientName);
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
                    new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            offsetRequestInfo.put(topicAndPartition,new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.EarliestTime(),1));

            final OffsetRequest offsetRequest = new OffsetRequest(offsetRequestInfo,
                    kafka.api.OffsetRequest.CurrentVersion(), clientName);
            final OffsetResponse offsetsBeforeResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
            if (offsetsBeforeResponse.hasError()){
                System.out.println("Error fetching data Offset Data the Broker. Reason: "+offsetsBeforeResponse.errorCode(topic,partition));
                return 0 ;
            }
            final long[] offsets = offsetsBeforeResponse.offsets(topic, partition);
            result = offsets[0];
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (simpleConsumer !=null){
                simpleConsumer.close();
            }
        }
        return result;
    }

    public long getLastOffset(String brokerHost,int port, String topic,int partition){
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try{
            simpleConsumer = new SimpleConsumer(brokerHost, port, 100000,
                    1024 * 1024, clientName);
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
                    new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            offsetRequestInfo.put(topicAndPartition,new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.LatestTime(),1));

            final OffsetRequest offsetRequest = new OffsetRequest(offsetRequestInfo,
                    kafka.api.OffsetRequest.CurrentVersion(), clientName);
            final OffsetResponse offsetsBeforeResponse = simpleConsumer.getOffsetsBefore(offsetRequest);
            if (offsetsBeforeResponse.hasError()){
                System.out.println("Error fetching data Offset Data the Broker. Reason: "+offsetsBeforeResponse.errorCode(topic,partition));
                return 0 ;
            }
            final long[] offsets = offsetsBeforeResponse.offsets(topic, partition);
            result = offsets[0];
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (simpleConsumer != null){
                simpleConsumer.close();
            }
        }
        return result;
    }

    public void readData(String brokerHost,int port,String topic,int partition,int offset,int fetchSize){
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        long readOffset = getLastOffset(brokerHost,port,topic,partition);
        try {
            simpleConsumer = new SimpleConsumer(brokerHost, port, 100000,
                    1024 * 1024, clientName);
            final FetchRequest fetchRequest = new FetchRequestBuilder()
                    .clientId(clientName).addFetch(topic, partition, offset, fetchSize).build();
            final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()){

            }
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic,partition)){
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset){
                    System.out.println("Found an old offset: "+currentOffset+" Expecting: "+readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                final ByteBuffer payload = messageAndOffset.message().payload();
                final byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                System.out.println(String.valueOf(messageAndOffset.offset()) +" : "+new String(bytes,"UTF-8"));
                numRead ++;
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (simpleConsumer != null){
                simpleConsumer.close();
            }
        }

    }
}
