package me.bliss.kafka.web.model;

import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 3/4/15
 *          Exp $
 */
public class Topic {

    private String name;

    private String version;

    private int replication;

    private int brokerSize;

    private Map<String,Integer> brokerPartitions;

    private Map<String,String> partitions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    public Map<String, Integer> getBrokerPartitions() {
        return brokerPartitions;
    }

    public void setBrokerPartitions(Map<String, Integer> brokerPartitions) {
        this.brokerPartitions = brokerPartitions;
    }

    public int getBrokerSize() {
        return brokerSize;
    }

    public void setBrokerSize(int brokerSize) {
        this.brokerSize = brokerSize;
    }

    public Map<String, String> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, String> partitions) {
        this.partitions = partitions;
    }
}
