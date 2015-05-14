package me.bliss.kafka.service;

import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.core.component.SimpleConsumerComponent;
import me.bliss.kafka.core.component.ZookeeperComponent;
import me.bliss.kafka.model.*;
import me.bliss.kafka.model.exception.SimpleConsumerLogicException;
import me.bliss.kafka.model.exception.ZookeeperException;
import me.bliss.kafka.model.result.ServiceResult;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 4/4/15
 *          Exp $
 */
public class TopicService {

    @Autowired
    private SimpleConsumerComponent simpleConsumerComponent;

    @Autowired
    private ZookeeperComponent zookeeperComponent;

    private SimpleConsumer simpleConsumer;

    private void init() {
        final List<ZKBroker> brokersList;
        try {
            brokersList = zookeeperComponent.getBrokersList();
            final ZKBroker zkBroker = brokersList.get(0);
            simpleConsumer = simpleConsumerComponent
                    .createSimpleSumer(zkBroker.getHost(), zkBroker.getPort());
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }

    }

    private void destory() {
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    public List<Topic> getAllTopics() {
        final ArrayList<Topic> allTopics = new ArrayList<Topic>();
        final List<String> zkTopicsList;
        try {
            zkTopicsList = zookeeperComponent.getTopicsList();
            for (String zkTopic : zkTopicsList) {
                final Topic topic = simpleConsumerComponent.getAllLeadersBySingleTopic(
                        simpleConsumer, zkTopic);
                allTopics.add(topic);
            }
        } catch (ZookeeperException e) {
            e.printStackTrace();
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        }

        return allTopics;
    }

    public List<TopicMessage> getAllMessages() {
        final ArrayList<TopicMessage> topicMessages = new ArrayList<TopicMessage>();
        final List<Topic> allTopics;
        try {
            allTopics = getAllTopics();
            for (Topic topic : allTopics) {
                if (StringUtils.equals(topic.getName(), "__consumer_offsets")) {
                    continue;
                }
                topicMessages.add(readTopicMessage(topic));
            }
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return topicMessages;
    }

    public ServiceResult<TopicMessage> getMessagesByTopic(String topicName){
        final ServiceResult<TopicMessage> serviceResult = new ServiceResult<TopicMessage>(true);
        final List<Topic> topics = getAllTopics();
        final Topic topic = findTopicByName(topics, topicName);
        if (topic == null){
            serviceResult.setSuccess(false);
            serviceResult.setErrorMsg("invalid topic name!");
            return serviceResult;
        }
        try {
            final TopicMessage topicMessage = readTopicMessage(topic);
            serviceResult.setResult(topicMessage);
            return serviceResult;
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Topic findTopicByName(List<Topic> topics,String topicName){
        for (Topic topic : topics){
            if (StringUtils.equals(topic.getName(),topicName)){
                return topic;
            }
        }
        return null;
    }

    private TopicMessage readTopicMessage(Topic topic)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final Iterator<Partitions> iterator = topic.getPartitionses().iterator();
        final TopicMessage topicMessage = new TopicMessage();
        final ArrayList<PartitionMessage> partitionMessages = new ArrayList<PartitionMessage>();
        while (iterator.hasNext()) {
            final Partitions partition = iterator.next();
            partitionMessages.add(readPartitionData(topic.getName(), partition, 10));
        }
        topicMessage.setName(topic.getName());
        topicMessage.setPartitionMessages(partitionMessages);
        return topicMessage;
    }

    private PartitionMessage readPartitionData(String topic, Partitions partition, int fetchSize)
            throws SimpleConsumerLogicException, UnsupportedEncodingException {
        final PartitionMessage partitionMessage = new PartitionMessage();
        final SimpleConsumer leaderSimpleConsumer = simpleConsumerComponent
                .getLeaderSimpleConsumer(simpleConsumer.host(), simpleConsumer.port(),
                        topic, partition.getId());
        final long earliestOffset = simpleConsumerComponent
                .getEarliestOffset(leaderSimpleConsumer, topic,
                        partition.getId());
        final long lastOffset = simpleConsumerComponent
                .getLastOffset(leaderSimpleConsumer, topic,
                        partition.getId());
        if (earliestOffset != lastOffset && earliestOffset < lastOffset) {
            int startOffset = (int) (lastOffset - 10 > 0 ?
                    lastOffset - 10 :
                    earliestOffset);
            final List<String> data = simpleConsumerComponent.readDataForPage(
                    leaderSimpleConsumer, topic, partition.getId(),
                    startOffset, fetchSize);
            partitionMessage.setId(partition.getId());
            partitionMessage.setMessages(data);
        }
        return partitionMessage;
    }

    public ServiceResult<Map<String, Object>> getKafkaEnvDetail() {
        final ServiceResult<Map<String, Object>> serviceResult = new ServiceResult<Map<String, Object>>(
                true);
        final HashMap<String, Object> map = new HashMap<String, Object>();
        try {
            final ZK zkDetail = zookeeperComponent.getZKDetail();
            final List<ZKBroker> brokersList = zookeeperComponent.getBrokersList();
            map.put("zookeeper", zkDetail);
            map.put("brokers", brokersList);
            map.put("topics", getAllTopics());
            serviceResult.setResult(map);
        } catch (ZookeeperException e) {
            serviceResult.setSuccess(false);
            serviceResult.setErrorMsg(e.getMessage());
            e.printStackTrace();
        }
        return serviceResult;
    }

    public ServiceResult<List<ZookeeperNode>> getNodesTree() {
        final ArrayList<ZookeeperNode> zookeeperNodes = new ArrayList<ZookeeperNode>();
        final ServiceResult<List<ZookeeperNode>> serviceResult = new ServiceResult<List<ZookeeperNode>>(
                true);
        try {
            zookeeperComponent.getAllNodes("/", zookeeperNodes);

            serviceResult.setResult(zookeeperNodes);
        } catch (ZookeeperException e) {
            serviceResult.setSuccess(false);
            serviceResult.setErrorMsg(e.getMessage());
            e.printStackTrace();
        }
        return serviceResult;
    }

    public void setSimpleConsumerComponent(
            SimpleConsumerComponent simpleConsumerComponent) {
        this.simpleConsumerComponent = simpleConsumerComponent;
    }
}
