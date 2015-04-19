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

    private int bufferSize = 1024 * 1024;

    /**
     *  send the topic query request to broker,respose topics detail info of partitions in topic from the broker
     *
     * @param host          active broker's host(ip/address)
     * @param port          active broker's port
     * @param topic         one topic of searched
     * @param partition     the partitions of topic
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(String host, int port,
                                               String topic, int partition)
            throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        Topic topicResult = null;
        try {
            simpleConsumer = createSimpleSumer(host, port);
            topicResult = getLeaderByTopicAndPartitions(simpleConsumer,
                    topic, partition);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return topicResult;
    }

    /**
     *   send the topic query request to broker,response topics detail info of partitions in topic from the broker
     * @param simpleConsumer  connected to borker's consumer
     * @param topic           one topic of searched
     * @param partition       the partitions of topic
     * @return
     */
    public Topic getLeaderByTopicAndPartitions(SimpleConsumer simpleConsumer, String topic,
                                               int partition) throws SimpleConsumerLogicException {
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
     * @param host       active broker's host(ip/address)
     * @param port       active broker's port
     * @param topic      one topic of searched
     * @return
     */
    public Topic getAllLeadersBySingleTopic(String host, int port,
                                            String topic) throws SimpleConsumerLogicException {

        SimpleConsumer simpleConsumer = null;
        Topic topicResult = null;
        try {
            simpleConsumer = createSimpleSumer(host, port);
            topicResult = getAllLeadersBySingleTopic(simpleConsumer, topic);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return topicResult;
    }

    /**
     *   send the topic query request to broker,respose topics detail info from the seedBroker
     * @param simpleConsumer  connected to borker's consumer
     * @param topic           one topic of searched
     * @return
     * @throws SimpleConsumerLogicException
     */
    public Topic getAllLeadersBySingleTopic(SimpleConsumer simpleConsumer, String topic)
            throws SimpleConsumerLogicException {
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(simpleConsumer,
                Collections.singletonList(topic));
        final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
        return handleTopicMetadata(topicMetadata);
    }

    /**
     *   send the topic query reqeust to broker,response topics detail info from the broker
     * @param host       active broker's host(ip/address)
     * @param port       active broker's port
     * @param topics     multi topics of searched
     * @return
     * @throws SimpleConsumerLogicException
     */
    public List<Topic> getAllLeadersByMultiTopics(String host, int port,
                                                  List<String> topics)
            throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        List<Topic> result = new ArrayList<Topic>();
        try {
            simpleConsumer = createSimpleSumer(host, port);
            result = getAllLeadersByMultiTopics(simpleConsumer,
                    topics);

        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return result;
    }

    /**
     *  send the topic query reqeust to broker,response topics detail info from the broker
     * @param simpleConsumer  connected to borker's consumer
     * @param topics          multi topics of searched
     * @return
     * @throws SimpleConsumerLogicException
     */
    public List<Topic> getAllLeadersByMultiTopics(SimpleConsumer simpleConsumer,
                                                  List<String> topics)
            throws SimpleConsumerLogicException {
        final ArrayList<Topic> result = new ArrayList<Topic>();
        final TopicMetadataResponse topicMetadataResponse = sendConsumerRequest(simpleConsumer,
                topics);
        final List<TopicMetadata> topicMetadatas = topicMetadataResponse.topicsMetadata();
        for (TopicMetadata topicMetadata : topicMetadatas) {
            result.add(handleTopicMetadata(topicMetadata));
        }
        return result;
    }

    /**
     *  send the topic query request to broker
     * @param host      active broker's host(ip/address)
     * @param port      active broker's port
     * @param topics    multi topics of searched
     * @return
     * @throws SimpleConsumerLogicException
     */
    private TopicMetadataResponse sendConsumerRequest(String host, int port,
                                                      List<String> topics)
            throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        TopicMetadataResponse topicMetadataResponse = null;
        try {
            simpleConsumer = createSimpleSumer(host, port);
            final TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
            topicMetadataResponse = simpleConsumer
                    .send(topicMetadataRequest);
        } catch (Exception e) {
            throw new SimpleConsumerLogicException(
                    "Error communicating with Broker [" + host + "] Reason: " + e);
        } finally {
            simpleConsumer.close();
        }
        return topicMetadataResponse;
    }

    /**
     *  send the topic query request to broker
     * @param simpleConsumer  connected to borker's consumer
     * @param topics          multi topics of searched
     * @return
     * @throws SimpleConsumerLogicException
     */
    private TopicMetadataResponse sendConsumerRequest(SimpleConsumer simpleConsumer,
                                                      List<String> topics)
            throws SimpleConsumerLogicException {
        TopicMetadataResponse topicMetadataResponse = null;
        final TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        topicMetadataResponse = simpleConsumer
                .send(topicMetadataRequest);
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
     *  query the earliest offset of one topic
     * @param host        active broker's host(ip/address)
     * @param port        active broker's port
     * @param topic       one topic of searched
     * @param partition   the partitions of topic
     * @return
     */
    public long getEarliestOffset(String host, int port, String topic, int partition)
            throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try {
            simpleConsumer = getLeaderSimpleConsumer(host, port, topic, partition);
            result = getEarliestOffset(simpleConsumer, topic, partition);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return result;
    }

    /**
     *  query the earliest offset of one topic
     * @param simpleConsumer  connected to borker's consumer
     * @param topic           one topic of searched
     * @param partition       the partitions of topic
     * @return
     */
    public long getEarliestOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
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
        return offsets[0];
    }

    /**
     *  query the last offset of one topic
     * @param host            active broker's host(ip/address)
     * @param port            active broker's port
     * @param topic           one topic of searched
     * @param partition       the partitions of topic
     * @return
     */
    public long getLastOffset(String host, int port, String topic, int partition)
            throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        long result = 0;
        try {
            simpleConsumer = getLeaderSimpleConsumer(host,port,topic,partition);
            result = getLastOffset(simpleConsumer,topic,partition);
        }finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
            }
        }
        return result;
    }

    /**
     *  query the last offset of one topic
     * @param simpleConsumer   connected to borker's consumer
     * @param topic            one topic of searched
     * @param partition        the partitions of topic
     * @return
     */
    public long getLastOffset(SimpleConsumer simpleConsumer, String topic, int partition) {
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
        return offsets[0];
    }

    /**
     *  reqd data from broker
     * @param host           active broker's host(ip/address)
     * @param port           active broker's port
     * @param topic          one topic of searched
     * @param partition      the partitions of topic
     * @param fetchSize      the record size of queryed
     * @throws SimpleConsumerLogicException
     */
    public List<String> readData(String host, int port, String topic, int partition, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        SimpleConsumer simpleConsumer = null;
        List<String> result = null;
        try {
            simpleConsumer = createSimpleSumer(host, port);
            result = readData(simpleConsumer, topic, partition, fetchSize);
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
     * @param fetchSize
     * @return
     * @throws SimpleConsumerLogicException
     * @throws UnsupportedEncodingException
     */
    public List<String> readData(SimpleConsumer simpleConsumer, String topic, int partition, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final ArrayList<String> result = new ArrayList<String>();
        final int entrySize = getEntrySize(simpleConsumer, topic, partition);
        if (entrySize == 0){
            //throw new SimpleConsumerLogicException("empty data");
            return result;
        }
        final FetchResponse fetchResponse = getFetchResponse(simpleConsumer, topic, partition,
                fetchSize * entrySize);
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
            System.out.println(String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
            result.add(String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
        }
        if (result.size() <= fetchSize){
            return result;
        }
        return result.subList(0,fetchSize);
    }

    /**
     *   reqd data from broker by paginator
     * @param simpleConsumer       connected to borker's consumer
     * @param topic                one topic of searched
     * @param partition            the partitions of topic
     * @param startOffset          start position of searched
     * @param fetchSize            the record size of queryed
     * @throws SimpleConsumerLogicException
     * @throws UnsupportedEncodingException
     */
    public List<String> readDataForPage(SimpleConsumer simpleConsumer, String topic, int partition,
                                int startOffset, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final ArrayList<String> result = new ArrayList<String>();
        final int entrySize = getEntrySize(simpleConsumer, topic, partition);
        final FetchResponse fetchResponse = getFetchResponse(simpleConsumer, topic, partition,
                startOffset, fetchSize  * entrySize);
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
            result.add(String.valueOf(messageAndOffset.offset()) + " : " + new String(bytes, "UTF-8"));
        }
        if (result.size() <= fetchSize){
            return result;
        }
        return result.subList(0,fetchSize);
    }

    /**
     *  get partition's leader connection
     * @param host          active broker's host(ip/address)
     * @param port          active broker's port
     * @param topic         one topic of searched
     * @param partition     the partitions of topic
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

        SimpleConsumer simpleConsumer = createSimpleSumer(leaderHost, leaderPort);
        ;
        return simpleConsumer;
    }

    /**
     *  get fetch response from broker
     * @param host            active broker's host(ip/address)
     * @param port            active broker's port
     * @param topic           one topic of searched
     * @param partition       the partitions of topic
     * @param fetchSize       the record size of queryed
     * @return
     * @throws SimpleConsumerLogicException
     */
    private FetchResponse getFetchResponse(String host, int port, String topic, int partition,
                                           int fetchSize) throws SimpleConsumerLogicException {
        SimpleConsumer simpleConsumer = null;
        FetchResponse fetchResponse = null;
        try {
            simpleConsumer = getLeaderSimpleConsumer(host, port, topic, partition);
            fetchResponse = getFetchResponse(simpleConsumer, topic, partition, fetchSize);
        } finally {
            if (simpleConsumer != null) {
                simpleConsumer.close();
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

    /**
     *  get fetch response from broker
     * @param simpleConsumer      connected to borker's consumer
     * @param topic               one topic of searched
     * @param partition           the partitions of topic
     * @param startOffset         start position of searched
     * @param fetchSize           the record size of queryed
     * @return
     * @throws SimpleConsumerLogicException
     */
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
     * @return
     * @throws SimpleConsumerLogicException
     */
    private int getEntrySize(SimpleConsumer simpleConsumer, String topic, int partition)
            throws SimpleConsumerLogicException {
        final FetchResponse fetchResponse = getFetchResponse(simpleConsumer, topic, partition,
                10000);
        int maxEntrySize = 0;
        final Iterator<MessageAndOffset> iterator = fetchResponse.messageSet(topic, partition)
                .iterator();
        while (iterator.hasNext()){
            final MessageAndOffset messageAndOffset = iterator.next();
            final int entrySize = MessageSet.entrySize(messageAndOffset.message());
            maxEntrySize = entrySize > maxEntrySize ? entrySize : maxEntrySize;
        }
        return maxEntrySize;
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
        final boolean hasNext = fetchResponse.messageSet(topic, partition).iterator().hasNext();
        if (hasNext) {
            return MessageSet.entrySize(
                    fetchResponse.messageSet(topic, partition).iterator().next().message());
        }
        return 0;
    }

    /**
     *
     * @param host
     * @param port
     * @return
     */
    public SimpleConsumer createSimpleSumer(String host, int port) {
        return new SimpleConsumer(host, port, timeout,
                bufferSize, clientName);
    }

}

