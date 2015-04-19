package me.bliss.kafka.web.service;

import com.google.gson.Gson;
import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.SimpleConsumerLogicComponent;
import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.component.model.Partitions;
import me.bliss.kafka.web.component.model.Topic;
import me.bliss.kafka.web.component.model.ZKBroker;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
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
    private SimpleConsumerLogicComponent simpleConsumerLogicComponent;

    private SimpleConsumer simpleConsumer;

    private void init() {
        try {
            final Gson gson = new Gson();
            final Map<Integer, String> brokersList = ZookeeperComponent.getBrokersList();
            final Set<Integer> brokerIds = brokersList.keySet();
            ZKBroker zkBroker = gson
                    .fromJson(brokersList.get(brokerIds.toArray()[0]), ZKBroker.class);
            simpleConsumer = simpleConsumerLogicComponent
                    .createSimpleSumer(zkBroker.getHost(), zkBroker.getPort());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void destory() {
        if (simpleConsumer != null) {
            simpleConsumer.close();
        }
    }

    public List<Topic> getAllTopics()
            throws KeeperException, InterruptedException, SimpleConsumerLogicException {
        final ArrayList<Topic> allTopics = new ArrayList<Topic>();
        final List<String> zkTopicsList = ZookeeperComponent.getTopicsList();
        for (String zkTopic : zkTopicsList) {
            final Topic topic = simpleConsumerLogicComponent
                    .getAllLeadersBySingleTopic(simpleConsumer, zkTopic);
            allTopics.add(topic);
        }
        return allTopics;
    }

    public Map<String, Map<Integer,List<String>>> getMessage(){
        final HashMap<String, Map<Integer,List<String>>> topicMessages = new HashMap<String, Map<Integer,List<String>>>();

        final List<Topic> allTopics;
        try {
            allTopics = getAllTopics();
            for (Topic topic : allTopics) {
                if (StringUtils.equals(topic.getName(),"__consumer_offsets")){
                    continue;
                }
                final Iterator<Partitions> iterator = topic.getPartitionses().iterator();
                final HashMap<Integer, List<String>> partitionMessages = new HashMap<Integer, List<String>>();
                while (iterator.hasNext()) {
                    final Partitions partitions = iterator.next();
                    simpleConsumerLogicComponent
                            .getLeaderSimpleConsumer(simpleConsumer.host(), simpleConsumer.port(),
                                    topic.getName(), partitions.getId());
                    final long earliestOffset = simpleConsumerLogicComponent
                            .getEarliestOffset(simpleConsumer, topic.getName(), partitions.getId());
                    final long lastOffset = simpleConsumerLogicComponent
                            .getLastOffset(simpleConsumer, topic.getName(), partitions.getId());
                    if (earliestOffset != lastOffset && earliestOffset < lastOffset) {
                        final List<String> data = simpleConsumerLogicComponent
                                .readData(simpleConsumer, topic.getName(), partitions.getId(), 10);
                        partitionMessages.put(partitions.getId(),data);
                    }

                }
                topicMessages.put(topic.getName(),partitionMessages);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return topicMessages;
    }

    public Map<String, Map<Integer,List<String>>> getMessagesForPage(){
        final HashMap<String, Map<Integer,List<String>>> topicMessages = new HashMap<String, Map<Integer,List<String>>>();

        try {
            final List<Topic> allTopics = getAllTopics();
            for (Topic topic : allTopics) {
                if (StringUtils.equals(topic.getName(),"__consumer_offsets")){
                    continue;
                }
                final Iterator<Partitions> iterator = topic.getPartitionses().iterator();
                final HashMap<Integer, List<String>> partitionMessages = new HashMap<Integer, List<String>>();
                while (iterator.hasNext()) {
                    final Partitions partitions = iterator.next();
                    simpleConsumerLogicComponent
                            .getLeaderSimpleConsumer(simpleConsumer.host(), simpleConsumer.port(),
                                    topic.getName(), partitions.getId());
                    final long earliestOffset = simpleConsumerLogicComponent
                            .getEarliestOffset(simpleConsumer, topic.getName(), partitions.getId());
                    final long lastOffset = simpleConsumerLogicComponent
                            .getLastOffset(simpleConsumer, topic.getName(), partitions.getId());
                    if (earliestOffset != lastOffset && earliestOffset < lastOffset) {
                        final List<String> data = simpleConsumerLogicComponent
                                .readData(simpleConsumer, topic.getName(), partitions.getId(), 10);
                        partitionMessages.put(partitions.getId(),data);
                    }

                }
                topicMessages.put(topic.getName(),partitionMessages);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return topicMessages;
    }

    public void setSimpleConsumerLogicComponent(
            SimpleConsumerLogicComponent simpleConsumerLogicComponent) {
        this.simpleConsumerLogicComponent = simpleConsumerLogicComponent;
    }
}
