package me.bliss.kafka.web.component;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import me.bliss.kafka.web.component.exception.SimpleConsumerLogicException;
import me.bliss.kafka.web.component.model.Partitions;
import me.bliss.kafka.web.component.model.Replication;
import me.bliss.kafka.web.component.model.Topic;

import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/26/15
 *          Exp $
 */
public class KafkaSimpleConsumerLogicComponent {

    private String clientName = "lanjue_kafka_"+new Date().getTime();

    private int timeout = 10000;

    /**
     *  send the topic query request to broker,respose topics detail info of partitions in topic from the seedBroker
     * @param seedBroker    active broker's host(ip/address)
     * @param port          active broker's port
     * @param seedTopic     one topic of searched
     * @param partition     the partitions of topic
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(String seedBroker, int port,
                                                           String seedTopic, int partition) {
        PartitionMetadata partitionMetadata = null;
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(seedBroker, port,
                Collections.singletonList(seedTopic));
        final List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
        final TopicMetadata topicMetadata = topicMetadatas.get(0);
        if (topicMetadata == null){
            return null;
        }
        final Topic topic = handleTopicMetadata(topicMetadata);
        for (Partitions partitions : topic.getPartitionses()){
            if (partitions.getId() == partition){
                final ArrayList<Partitions> partitionses = new ArrayList<Partitions>();
                partitionses.add(partitions);
                topic.setPartitionses(partitionses);
            }
        }
        return topic;
    }

    /**
     *
     * @param simpleConsumer
     * @param seedTopic
     * @param partition
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(SimpleConsumer simpleConsumer, String seedTopic, int partition){
        return getLeaderByTopicAndPartitions(simpleConsumer.host(),simpleConsumer.port(),seedTopic,partition);
    }

    /**
     *   send the topic query request to broker,respose topics detail info from the seedBroker
     * @param seedBroker    active broker's host(ip/address)
     * @param port          active broker's port
     * @param seedtopic     one topic of searched
     * @return
     */
    public Topic getAllLeadersBySingleTopic(String seedBroker, int port,
                                            String seedtopic) {
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(seedBroker, port,
                Collections.singletonList(seedtopic));
        final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
        return handleTopicMetadata(topicMetadata);
    }

    /**
     *
     * @param simpleConsumer
     * @param seedTopic
     * @return
     */
    public Topic getAllLeadersBySingleTopic(SimpleConsumer simpleConsumer,String seedTopic){
        return getAllLeadersBySingleTopic(simpleConsumer.host(), simpleConsumer.port(), seedTopic);
    }

    /**
     *   send the topic query request to broker,respose topics detail info from the seedBroker
     * @param seedBroker    active broker's host(ip/address)
     * @param port          active broker's port
     * @param topics        the topic of searched
     * @return
     */
    public List<Topic> getAllLeadersByMultiTopics(String seedBroker, int port,
                                                  List<String> topics) {
        final ArrayList<Topic> result = new ArrayList<Topic>();
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(seedBroker, port,
                topics);
        final List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
        for (TopicMetadata topicMetadata : topicMetadatas) {
            result.add(handleTopicMetadata(topicMetadata));
        }
        return result;
    }

    /**
     *
     * @param simpleConsumer
     * @param topics
     * @return
     */
    public List<Topic> getAllLeadersByMultiTopics(SimpleConsumer simpleConsumer,List<String> topics){
        return getAllLeadersByMultiTopics(simpleConsumer.host(),simpleConsumer.port(),topics);
    }

    /**
     *   send the topic query request to broker,respose topics detail info from the seedBroker
     * @param seedBroker    active broker's host(ip/address)
     * @param port          active broker's port
     * @param topics        the topic of search
     * @return
     */
    private TopicMetadataResponse sendConsumerRequest(String seedBroker, int port,
                                                     List<String> topics) {
        SimpleConsumer simpleConsumer = null;
        TopicMetadataResponse topicMetadataResponse = null;
        try {
            simpleConsumer = new SimpleConsumer(seedBroker, port, timeout,
                    1024 * 1024, clientName);
            final TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
            topicMetadataResponse = simpleConsumer
                    .send(topicMetadataRequest);
        } catch (Exception e) {
            System.out.println("Error communicating with Broker [" + seedBroker + "] Reason: " + e);
        } finally {
            simpleConsumer.close();
        }
        return topicMetadataResponse;
    }


    /**
     *  TopicMetadata warpper to Topic
     * @param topicMetadata
     * @return
     */
    private Topic handleTopicMetadata(TopicMetadata topicMetadata) {
        final Topic topic = new Topic();
        topic.setName(topicMetadata.topic());
        final ArrayList<Partitions> partitionses = new ArrayList<Partitions>();
        for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
            final Partitions partitions = new Partitions();
            partitions.setId(partitionMetadata.partitionId());
            final Replication leader = new Replication();
            leader.setId(partitionMetadata.leader().id());
            leader.setHost(partitionMetadata.leader().host());
            leader.setPort(partitionMetadata.leader().port());
            partitions.setLeader(leader);

            final List<Replication> replicas = new ArrayList<Replication>();
            for (Broker replica : partitionMetadata.replicas()) {
                final Replication replication = new Replication();
                replication.setId(replica.id());
                replication.setHost(replica.host());
                replication.setPort(replica.port());
                replicas.add(replication);
            }
            partitions.setReplicas(replicas);
            partitionses.add(partitions);
        }
        topic.setPartitionses(partitionses);
        return topic;
    }

    /**
     *
     * @param brokerHost
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public long getEarliestOffset(String brokerHost,int port, String topic,int partition) {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try{
            simpleConsumer = new SimpleConsumer(brokerHost, port, timeout,
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

    /**
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @return
     */
    public long getEarliestOffset(SimpleConsumer simpleConsumer,String topic,int partition){
        return getEarliestOffset(simpleConsumer.host(),simpleConsumer.port(),topic,partition);
    }

    /**
     *
     * @param brokerHost
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public long getLastOffset(String brokerHost,int port, String topic,int partition){
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try{
            simpleConsumer = new SimpleConsumer(brokerHost, port, timeout,
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

    /**
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @return
     */
    public long getLastOffset(SimpleConsumer simpleConsumer,String topic,int partition){
        return getLastOffset(simpleConsumer.host(),simpleConsumer.port(),topic,partition);
    }

    /**
     *
     * @param brokerHost
     * @param port
     * @param topic
     * @param partition
     * @param fetchSize
     * @throws SimpleConsumerLogicException
     */
    public void readData(String brokerHost,int port,String topic,int partition,int fetchSize)
            throws SimpleConsumerLogicException {
        final Partitions leader = getLeaderByTopicAndPartitions(brokerHost, port,
                topic, partition).getPartitionses().get(0);
        if (leader == null){
            throw new SimpleConsumerLogicException("Can't find metadata for Topic and Partition. Exiting");
        }
        String leaderHost = leader.getLeader().getHost();
        int leaderPort = leader.getLeader().getPort();

        SimpleConsumer simpleConsumer = null;

        try {
            simpleConsumer = new SimpleConsumer(leaderHost, leaderPort, timeout,
                    1024 * 1024, clientName);
            long readOffset = getEarliestOffset(leaderHost, leaderPort, topic, partition);

            int numErrors = 0;

            final kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder()
                    .clientId(clientName).addFetch(topic, partition, readOffset, fetchSize).build();

            final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
            if (fetchResponse.hasError()){
                numErrors ++;
                final short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the broker: "+leaderHost + " Reason: "+code);
                if (code == ErrorMapping.OffsetOutOfRangeCode()){
                    System.out.println();
                }
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

