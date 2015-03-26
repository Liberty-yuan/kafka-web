package me.bliss.kafka.web.component;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.model.Partitions;
import me.bliss.kafka.web.component.model.Replication;
import me.bliss.kafka.web.component.model.Topic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/26/15
 *          Exp $
 */
public class KafkaPartitionsLeaderComponent {

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
            simpleConsumer = new SimpleConsumer(seedBroker, port, 100000,
                    1024 * 1024, String.valueOf(new Date().getTime()));
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

}

