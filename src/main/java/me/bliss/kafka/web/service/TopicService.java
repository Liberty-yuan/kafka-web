package me.bliss.kafka.web.service;

import com.google.gson.Gson;
import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.SimpleConsumerComponent;
import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.component.model.*;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
import me.bliss.kafka.web.exception.ZookeeperException;
import me.bliss.kafka.web.result.ServiceResult;
import org.apache.commons.lang.ArrayUtils;
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
        final Gson gson = new Gson();
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
                final Topic topic = simpleConsumerComponent
                        .getAllLeadersBySingleTopic(simpleConsumer, zkTopic);
                allTopics.add(topic);
            }
        } catch (ZookeeperException e) {
            e.printStackTrace();
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        }

        return allTopics;
    }

    public List<TopicMessage> getMessage() {
        final ArrayList<TopicMessage> topicMessages = new ArrayList<TopicMessage>();
        final List<Topic> allTopics;
        try {
            allTopics = getAllTopics();
            for (Topic topic : allTopics) {
                if (StringUtils.equals(topic.getName(), "__consumer_offsets")) {
                    continue;
                }
                final Iterator<Partitions> iterator = topic.getPartitionses().iterator();
                final TopicMessage topicMessage = new TopicMessage();
                final ArrayList<PartitionMessage> partitionMessages = new ArrayList<PartitionMessage>();
                while (iterator.hasNext()) {
                    final PartitionMessage partitionMessage = new PartitionMessage();
                    final Partitions partitions = iterator.next();
                    simpleConsumerComponent
                            .getLeaderSimpleConsumer(simpleConsumer.host(), simpleConsumer.port(),
                                    topic.getName(), partitions.getId());
                    final long earliestOffset = simpleConsumerComponent
                            .getEarliestOffset(simpleConsumer, topic.getName(), partitions.getId());
                    final long lastOffset = simpleConsumerComponent
                            .getLastOffset(simpleConsumer, topic.getName(), partitions.getId());
                    if (earliestOffset != lastOffset && earliestOffset < lastOffset) {
                        int startOffset = (int) (lastOffset - 10 > 0 ? lastOffset -10 : earliestOffset);
                        final List<String> data = simpleConsumerComponent.readDataForPage(
                                simpleConsumer, topic.getName(), partitions.getId(),
                                startOffset, 10);
                        partitionMessage.setId(partitions.getId());
                        partitionMessage.setMessages(data);
                        partitionMessages.add(partitionMessage);
                    }

                }
                topicMessage.setName(topic.getName());
                topicMessage.setPartitionMessages(partitionMessages);
                topicMessages.add(topicMessage);
            }
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return topicMessages;
    }

    public List<TopicMessage> getMessagesByReverse(){
        final List<TopicMessage> topicMessages = getMessage();
        ArrayUtils.reverse(topicMessages.toArray());
        return topicMessages;
    }

    public Map<String, Map<Integer, List<String>>> getMessagesForPage(int pageNum, int pageSize) {
        final HashMap<String, Map<Integer, List<String>>> topicMessages = new HashMap<String, Map<Integer, List<String>>>();

        try {
            final List<Topic> allTopics = getAllTopics();
            for (Topic topic : allTopics) {
                if (StringUtils.equals(topic.getName(), "__consumer_offsets")) {
                    continue;
                }
                final Iterator<Partitions> iterator = topic.getPartitionses().iterator();
                final HashMap<Integer, List<String>> partitionMessages = new HashMap<Integer, List<String>>();
                while (iterator.hasNext()) {
                    final Partitions partitions = iterator.next();
                    simpleConsumerComponent
                            .getLeaderSimpleConsumer(simpleConsumer.host(), simpleConsumer.port(),
                                    topic.getName(), partitions.getId());
                    final long earliestOffset = simpleConsumerComponent
                            .getEarliestOffset(simpleConsumer, topic.getName(), partitions.getId());
                    final long lastOffset = simpleConsumerComponent
                            .getLastOffset(simpleConsumer, topic.getName(), partitions.getId());
                    if (earliestOffset != lastOffset && earliestOffset < lastOffset) {
                        final List<String> data = simpleConsumerComponent
                                .readData(simpleConsumer, topic.getName(), partitions.getId(), 10);
                        partitionMessages.put(partitions.getId(), data);
                    }

                }
                topicMessages.put(topic.getName(), partitionMessages);
            }
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return topicMessages;
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
