package me.bliss.kafka.web.component;

import com.google.gson.Gson;
import me.bliss.kafka.web.component.model.ZK;
import me.bliss.kafka.web.component.model.ZKBroker;
import me.bliss.kafka.web.component.model.ZKTopic;
import me.bliss.kafka.web.constant.ServiceContants;
import me.bliss.kafka.web.exception.ZookeeperException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/3/15
 *          Exp $
 */

public class ZookeeperComponent {

    private String host = "qingting.manage.alipay.net";

    private int port = 2181;

    private int timeout = 60000;

    private ZooKeeper zooKeeper = null;

    private WatchZookeeperStatus watchZookeeperStatus;

    private void init() {
        try {
            zooKeeper = new ZooKeeper(host + ":" + port, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("触发了事件" + event.getType());
                }
            });
            watchZookeeperStatus = new WatchZookeeperStatus();
        } catch (IOException e) {
            throw new RuntimeException("初始化链接zookeeper出错");
        }
    }

    public String create(String path, String data) throws ZookeeperException {
        try {
            final Stat exists = zooKeeper.exists(path, watchZookeeperStatus);
            if (exists != null && exists.getDataLength() > 0) {
                throw new ZookeeperException("待创建的路径已经存在");
            }
            return zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            throw new ZookeeperException("创建节点失败");
        }
    }

    public String getData(String path) throws ZookeeperException {
        try {
            byte[] data = zooKeeper.getData(path, watchZookeeperStatus, null);
            return new String(data);
        } catch (Exception e) {
            throw new ZookeeperException("获取数据失败");
        }
    }

    public boolean deleteNode(String path) throws ZookeeperException {
        try {
            zooKeeper.delete(path, -1);
            return true;
        } catch (Exception e) {
            throw new ZookeeperException("删除节点失败");
        }
    }

    public List<String> getTopicsList() throws ZookeeperException {
        try {
            return zooKeeper
                    .getChildren(ServiceContants.KAFKA_BROKERS_TOPIC_PATH, watchZookeeperStatus);
        } catch (Exception e) {
            throw new ZookeeperException("获取数据失败");
        }
    }

    public List<ZKTopic> getTopicsDetail() throws ZookeeperException {
        final ArrayList<ZKTopic> zkTopics = new ArrayList<ZKTopic>();
        final List<String> topicsList = getTopicsList();
        for (String topic : topicsList) {
            final ZKTopic zkTopic = new ZKTopic();
            //获取该topic的partitions集合
            final List<String> partitions = getChildren(
                    ServiceContants.KAFKA_BROKERS_TOPIC_PATH + "/" + topic + "/partitions");
            final ArrayList<Integer> targetPartitions = new ArrayList<Integer>();
            if (!CollectionUtils.isEmpty(partitions)) {
                for (String partition : partitions) {
                    targetPartitions.add(Integer.parseInt(partition));
                }
            }
            zkTopic.setName(topic);
            zkTopic.setPartitions(targetPartitions);
            zkTopics.add(zkTopic);
        }
        return zkTopics;
    }

    public List<String> getChildren(String path) throws ZookeeperException {
        try {
            return zooKeeper.getChildren(path, watchZookeeperStatus);
        } catch (Exception e) {
            throw new ZookeeperException("获取子节点失败");
        }
    }

    public List<ZKBroker> getBrokersList() throws ZookeeperException {
        final ArrayList<ZKBroker> zkBrokers = new ArrayList<ZKBroker>();
        final Gson gson = new Gson();
        final List<String> brokerIds = getChildren(ServiceContants.KAFKA_BROKERS_IDS_PATH);

        for (String brokerId : brokerIds) {
            final String data = getData(ServiceContants.KAFKA_BROKERS_IDS_PATH + "/" + brokerId);
            final ZKBroker zkBroker = gson.fromJson(data, ZKBroker.class);
            zkBroker.setId(Integer.parseInt(brokerId));
            zkBrokers.add(zkBroker);
        }
        return zkBrokers;
    }

    public boolean isExists(String path) throws ZookeeperException {
        try {
            final Stat exists = zooKeeper.exists(path, watchZookeeperStatus);
            return exists != null ? true : false;
        } catch (Exception e) {
            throw new ZookeeperException("判断路径是否存在异常");
        }
    }

    public Stat getNodeState(String path) throws ZookeeperException {
        try {
            return zooKeeper.exists(path, watchZookeeperStatus);
        } catch (Exception e) {
            throw new ZookeeperException("获取节点状态异常");
        }
    }

    public ZK getZKDetail() throws ZookeeperException {
        final ZK zk = new ZK();
        zk.setSessionId(zooKeeper.getSessionId());
        zk.setSessionTimeOut(zooKeeper.getSessionTimeout());
        zk.setHost(host);
        zk.setPort(port);
        return zk;
    }

    static class WatchZookeeperStatus implements Watcher {
        @Override public void process(WatchedEvent event) {
            System.out.println(
                    "emit the get topic event " + event.getType() + " and state is "
                    + event.getState());
        }
    }

}
