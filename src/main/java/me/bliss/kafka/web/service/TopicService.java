package me.bliss.kafka.web.service;

import com.google.gson.Gson;
import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.SimpleConsumerComponent;
import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.component.model.Partitions;
import me.bliss.kafka.web.component.model.Topic;
import me.bliss.kafka.web.component.model.ZKBroker;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
import me.bliss.kafka.web.exception.ZookeeperException;
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

    public Map<String, Map<Integer, List<String>>> getMessage() {
        final HashMap<String, Map<Integer, List<String>>> topicMessages = new HashMap<String, Map<Integer, List<String>>>();

        final List<Topic> allTopics;
        try {
            allTopics = getAllTopics();
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

    public void setSimpleConsumerComponent(
            SimpleConsumerComponent simpleConsumerComponent) {
        this.simpleConsumerComponent = simpleConsumerComponent;
    }
}
