package me.bliss.kafka.web.service;

import me.bliss.kafka.web.constant.ServiceContants;
import me.bliss.kafka.web.model.Broker;
import me.bliss.kafka.web.model.Topic;
import me.bliss.kafka.web.result.ServiceResult;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/3/15
 *          Exp $
 */

public class ZookeeperService {

    private static String host = "10.210.12.204";

    private static int port = 2181;

    private static int timeout = 60000;

    private static ZooKeeper zooKeeper = null;

    static {
        try {
            zooKeeper = new ZooKeeper(host + ":" + port, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("触发了事件" + event.getType());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ServiceResult<String> create(String path, String data) {
        final ServiceResult<String> result = new ServiceResult<String>();
        try {
            final Stat exists = zooKeeper.exists(path, false);
            if (exists != null && exists.getDataLength() > 0) {
                result.setSuccess(false);
                result.setErrorMsg("path Already exists");
                return result;
            }
            String createResult = zooKeeper
                    .create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
            result.setSuccess(true);
            result.setResult(createResult);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static ServiceResult<String> getData(String path) {
        final ServiceResult<String> result = new ServiceResult<String>();
        try {
            byte[] data = zooKeeper.getData(path, false, null);
            result.setResult(new String(data));
            result.setSuccess(true);
        } catch (Exception e) {
            result.setSuccess(false);
            result.setErrorMsg(e.getMessage());
        }
        return result;
    }

    public static ServiceResult deleteNode(String path) {
        final ServiceResult<Boolean> result = new ServiceResult<Boolean>();
        try {
            zooKeeper.delete(path, -1);
            result.setResult(true);
            result.setSuccess(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static ServiceResult<List<String>> getChildren(String path) {
        final ServiceResult<List<String>> result = new ServiceResult<List<String>>();

        final ArrayList<String> nodes = new ArrayList<String>();
        return result;
    }

    public static ServiceResult<Map<String,Object>> getBrokers() {
        final ServiceResult<Map<String, Object>> serviceResult = new ServiceResult<Map<String, Object>>();
        final HashMap<String, Object> brokers = new HashMap<String, Object>();
        final HashMap<String, Broker> ids = new HashMap<String, Broker>();
        final List<Topic> topics = new ArrayList<Topic>();
        final ObjectMapper mapper = new ObjectMapper();
        try {
            final List<String> idsChildren = zooKeeper
                    .getChildren(ServiceContants.KAFKA_BROKERS_IDS_PATH, false);
            for (String idsChild : idsChildren) {
                final byte[] bytes = zooKeeper
                        .getData(ServiceContants.KAFKA_BROKERS_IDS_PATH + "/" + idsChild, false,
                                null);
                ids.put(idsChild,mapper.readValue(new String(bytes), Broker.class));
                //ids.put(idsChild, gson.fromJson(new String(bytes), Map.class));
            }
            final List<String> topicsChildren = zooKeeper
                    .getChildren(ServiceContants.KAFKA_BROKERS_TOPIC_PATH, false);
            for (String topicChild : topicsChildren) {
                final byte[] bytes = zooKeeper
                        .getData(ServiceContants.KAFKA_BROKERS_TOPIC_PATH + "/" + topicChild, false,
                                null);
                final JsonNode root = mapper.readTree(new String(bytes));
                final HashMap<String, String> partitions = new HashMap<String, String>();
                final HashMap<String, Integer> brokerPartitions = new HashMap<String, Integer>();
                final Topic topic = new Topic();


                final Iterator<String> fieldNames = root.findPath("partitions").getFieldNames();

                int replication = 0;
                while (fieldNames.hasNext()) {
                    final String field = fieldNames.next();
                    final String partition = root.findPath("partitions").get(field).toString();
                    accumulateBrokerPartitions(partition, brokerPartitions);
                    partitions.put(field, removeStartAndEndChar(partition));
                    replication = judgeReplication(partition,replication);
                }

                topic.setName(topicChild);
                topic.setPartitions(partitions);
                topic.setVersion(root.findPath("partitions").getTextValue());
                topic.setReplication(replication);
                topic.setBrokerPartitions(brokerPartitions);

                topics.add(topic);

            }
            brokers.put("ids", ids);
            brokers.put("topics", topics);
            serviceResult.setSuccess(true);
            serviceResult.setResult(brokers);
        } catch (Exception e) {
            e.printStackTrace();
            serviceResult.setSuccess(false);
            serviceResult.setErrorMsg("interface invoke error!");
        }
        return serviceResult;
    }

    private static String removeStartAndEndChar(String partition){
        return partition.substring(1,partition.length()-1);
    }

    private static int judgeReplication(String parition,int replication){
        final int length = parition.split(",").length;
        return length > replication ? length : replication;
    }

    private static void accumulateBrokerPartitions(String data,Map<String,Integer> brokersPartitions){
        final String field =removeStartAndEndChar(data);
        final String[] keys = field.split(",");
        for (String key : keys){
            if (brokersPartitions.containsKey(key)){
                int count = brokersPartitions.get(key);
                brokersPartitions.put(key,count+1);
            }else {
                brokersPartitions.put(key, 1);
            }
        }

    }

    private static String convertListToString(List<String> lists) {
        final StringBuffer stringBuffer = new StringBuffer();
        final Iterator<String> iterator = lists.iterator();
        while (iterator.hasNext()) {
            stringBuffer.append("/").append(iterator.next());
        }
        return stringBuffer.toString();
    }

}
