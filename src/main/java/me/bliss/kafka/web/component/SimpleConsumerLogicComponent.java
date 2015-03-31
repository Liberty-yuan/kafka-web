package me.bliss.kafka.web.component;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import me.bliss.kafka.web.component.model.Partitions;
import me.bliss.kafka.web.component.model.Replication;
import me.bliss.kafka.web.component.model.Topic;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/26/15
 *          Exp $
 */
public class SimpleConsumerLogicComponent {

    private String clientName = "lanjue_kafka_" + new Date().getTime();

    private int timeout = 10000;

    /**
     *  send the topic query request to broker,respose topics detail info of partitions in topic from the seedBroker
     * @param host    active broker's host(ip/address)
     * @param port          active broker's port
     * @param topic     one topic of searched
     * @param partition     the partitions of topic
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(String host, int port,
                                               String topic, int partition) {
        SimpleConsumer simpleConsumer = null;
        Topic topicResult = null;
        try{
            simpleConsumer = new SimpleConsumer(host, port, timeout,
                    1024 * 1024, clientName);
            topicResult = getLeaderByTopicAndPartitions(simpleConsumer,
                    topic, partition);
        }finally {
            if (simpleConsumer != null){
                simpleConsumer.close();
            }
        }
        return topicResult;
    }

    /**
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(SimpleConsumer simpleConsumer, String topic,
                                               int partition) {
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(simpleConsumer,
                Collections.singletonList(topic));
        final List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
        final TopicMetadata topicMetadata = topicMetadatas.get(0);
        if (topicMetadata == null) {
            return null;
        }
        final Topic resultTopic = handleTopicMetadata(topicMetadata);
        for (Partitions partitions : resultTopic.getPartitionses()) {
            if (partitions.getId() == partition) {
                final ArrayList<Partitions> partitionses = new ArrayList<Partitions>();
                partitionses.add(partitions);
                resultTopic.setPartitionses(partitionses);
            }
        }
        return resultTopic;
    }

    /**
     *   send the topic query request to broker,respose topics detail info from the seedBroker
     * @param host    active broker's host(ip/address)
     * @param port          active broker's port
     * @param topic     one topic of searched
     * @return
     */
    public Topic getAllLeadersBySingleTopic(String host, int port,
                                            String topic) {

        SimpleConsumer simpleConsumer = null;
        Topic topicResult = null;
        try{
            simpleConsumer = new SimpleConsumer(host, port, timeout,
                    1024 * 1024, clientName);
            topicResult = getAllLeadersBySingleTopic(simpleConsumer, topic);
        }finally {
            if (simpleConsumer != null){
                simpleConsumer.close();
            }
        }
        return topicResult;
    }


    public Topic getAllLeadersBySingleTopic(SimpleConsumer simpleConsumer, String topic) {
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(simpleConsumer,
                Collections.singletonList(topic));
        final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
        return handleTopicMetadata(topicMetadata);
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
    public List<Topic> getAllLeadersByMultiTopics(SimpleConsumer simpleConsumer,
                                                  List<String> topics) {
        return getAllLeadersByMultiTopics(simpleConsumer.host(), simpleConsumer.port(), topics);
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



    private TopicMetadataResponse sendConsumerRequest(SimpleConsumer simpleConsumer,
                                                      List<String> topics) {
        TopicMetadataResponse topicMetadataResponse = null;
        final TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        topicMetadataResponse = simpleConsumer
                .send(topicMetadataRequest);
        System.out.println(
                "Error communicating with Broker [" + simpleConsumer.host() + "] Reason: ");
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
    public long getEarliestOffset(String brokerHost, int port, String topic, int partition) {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try {
            simpleConsumer = new SimpleConsumer(brokerHost, port, timeout,
                    1024 * 1024, clientName);
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
                    new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            offsetRequestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.EarliestTime(), 1));

            final OffsetRequest offsetRequest = new OffsetRequest(offsetRequestInfo,
                    kafka.api.OffsetRequest.CurrentVersion(), clientName);
            final OffsetResponse offsetsBeforeResponse = simpleConsumer
                    .getOffsetsBefore(offsetRequest);
            if (offsetsBeforeResponse.hasError()) {
                System.out.println("Error fetching data Offset Data the Broker. Reason: "
                                   + offsetsBeforeResponse.errorCode(topic, partition));
                return 0;
            }
            final long[] offsets = offsetsBeforeResponse.offsets(topic, partition);
            result = offsets[0];
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (simpleConsumer != null) {
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
    public long getEarliestOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
        return getEarliestOffset(simpleConsumer.host(), simpleConsumer.port(), topic, partition);
    }

    /**
     *
     * @param brokerHost
     * @param port
     * @param topic
     * @param partition
     * @return
     */
    public long getLastOffset(String brokerHost, int port, String topic, int partition) {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try {
            simpleConsumer = new SimpleConsumer(brokerHost, port, timeout,
                    1024 * 1024, clientName);
            final TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            final Map<TopicAndPartition, PartitionOffsetRequestInfo> offsetRequestInfo =
                    new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            offsetRequestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                    kafka.api.OffsetRequest.LatestTime(), 1));

            final OffsetRequest offsetRequest = new OffsetRequest(offsetRequestInfo,
                    kafka.api.OffsetRequest.CurrentVersion(), clientName);
            final OffsetResponse offsetsBeforeResponse = simpleConsumer
                    .getOffsetsBefore(offsetRequest);
            if (offsetsBeforeResponse.hasError()) {
                System.out.println("Error fetching data Offset Data the Broker. Reason: "
                                   + offsetsBeforeResponse.errorCode(topic, partition));
                return 0;
            }
            final long[] offsets = offsetsBeforeResponse.offsets(topic, partition);
            result = offsets[0];
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (simpleConsumer != null) {
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
    public long getLastOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
        return getLastOffset(simpleConsumer.host(), simpleConsumer.port(), topic, partition);
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
    public void readData(String brokerHost, int port, String topic, int partition, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final int entrySize = getEntrySize(brokerHost, port, topic, partition);
        final FetchResponse fetchResponse = getFetchResponse(brokerHost, port, topic, partition,
                fetchSize * entrySize);
        long readOffset = getEarliestOffset(brokerHost, port, topic, partition);
        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                System.out.println(
                        "Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            readOffset = messageAndOffset.nextOffset();
            final ByteBuffer payload = messageAndOffset.message().payload();
            final byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(
                    String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
        }

    }

    public void readDataForPage(SimpleConsumer simpleConsumer, String topic, int partition,
                                int startOffset, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final int entrySize = getEntrySize(simpleConsumer, topic, partition);
        final FetchResponse fetchResponse = getFetchResponse(simpleConsumer, topic, partition,
                startOffset, (fetchSize - startOffset) * entrySize);
        long readOffset = getEarliestOffset(simpleConsumer, topic, partition);

        for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                System.out.println(
                        "Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            readOffset = messageAndOffset.nextOffset();
            final ByteBuffer payload = messageAndOffset.message().payload();
            final byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(
                    String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
        }
    }

    /**
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     * @throws SimpleConsumerLogicException
     */
    public SimpleConsumer getLeaderSimpleConsumer(String host, int port, String topic,
                                                  int partition)
            throws SimpleConsumerLogicException {
        final Partitions leader = getLeaderByTopicAndPartitions(host, port,
                topic, partition).getPartitionses().get(0);
        if (leader == null) {
            throw new SimpleConsumerLogicException(
                    "Can't find metadata for Topic and Partition. Exiting");
        }
        String leaderHost = leader.getLeader().getHost();
        int leaderPort = leader.getLeader().getPort();

        SimpleConsumer simpleConsumer = new SimpleConsumer(leaderHost, leaderPort, timeout,
                1024 * 1024, clientName);
        ;
        return simpleConsumer;
    }

    /**
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @param fetchSize
     * @return
     * @throws SimpleConsumerLogicException
     */
    private FetchResponse getFetchResponse(String host, int port, String topic, int partition,
                                           int fetchSize) throws SimpleConsumerLogicException {
        final SimpleConsumer simpleConsumer = getLeaderSimpleConsumer(host, port, topic,
                partition);
        long readOffset = getEarliestOffset(simpleConsumer, topic, partition);
        final kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder()
                .clientId(clientName).addFetch(topic, partition, readOffset, fetchSize).build();
        final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
        if (fetchResponse.hasError()) {
            final short code = fetchResponse.errorCode(topic, partition);
            System.out.println(
                    "Error fetching data from the broker: " + simpleConsumer.host() + " Reason: "
                    + code);
            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                System.out.println();
            }
        }
        return fetchResponse;
    }

    private FetchResponse getFetchResponse(SimpleConsumer simpleConsumer, String topic,
                                           int partition, int startOffset, int fetchSize)
            throws SimpleConsumerLogicException {
        long readOffset = getEarliestOffset(simpleConsumer, topic, partition);
        if (startOffset < readOffset) {
            throw new SimpleConsumerLogicException("start offset error, invalid offset");
        }
        final kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder()
                .clientId(clientName).addFetch(topic, partition, startOffset, fetchSize).build();
        final FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
        if (fetchResponse.hasError()) {
            final short code = fetchResponse.errorCode(topic, partition);
            System.out.println(
                    "Error fetching data from the broker: " + simpleConsumer.host() + " Reason: "
                    + code);
            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                System.out.println();
            }
        }
        return fetchResponse;
    }

    /**
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @param fetchSize
     * @return
     * @throws SimpleConsumerLogicException
     */
    private FetchResponse getFetchResponse(SimpleConsumer simpleConsumer, String topic,
                                           int partition, int fetchSize)
            throws SimpleConsumerLogicException {
        return getFetchResponse(simpleConsumer.host(), simpleConsumer.port(), topic, partition,
                fetchSize);
    }

    /**
     *
     * @param simpleConsumer
     * @param topic
     * @param partition
     * @return
     * @throws SimpleConsumerLogicException
     */
    private int getEntrySize(SimpleConsumer simpleConsumer, String topic, int partition)
            throws SimpleConsumerLogicException {
        final FetchResponse fetchResponse = getFetchResponse(simpleConsumer, topic, partition,
                10000);
        return MessageSet.entrySize(
                fetchResponse.messageSet(topic, partition).iterator().next().message());
    }

    /**
     *
     * @param host
     * @param port
     * @param topic
     * @param partition
     * @return
     * @throws SimpleConsumerLogicException
     */
    private int getEntrySize(String host, int port, String topic, int partition)
            throws SimpleConsumerLogicException {
        final FetchResponse fetchResponse = getFetchResponse(host, port, topic, partition,
                10000);
        return MessageSet.entrySize(
                fetchResponse.messageSet(topic, partition).iterator().next().message());
    }

}

