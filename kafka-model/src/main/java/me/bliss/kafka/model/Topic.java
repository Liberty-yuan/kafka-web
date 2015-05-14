package me.bliss.kafka.model;

import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model.consumer, v 0.1 3/26/15
 *          Exp $
 */
public class Topic {

    private String name;

    private List<Partitions> partitionses;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Partitions> getPartitionses() {
        return partitionses;
    }

    public void setPartitionses(List<Partitions> partitionses) {
        this.partitionses = partitionses;
    }
}
