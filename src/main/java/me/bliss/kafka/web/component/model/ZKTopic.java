package me.bliss.kafka.web.component.model;

import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.model, v 0.1 5/11/15
 *          Exp $
 */
public class ZKTopic {

    private String name;

    private List<Integer> partitions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Integer> partitions) {
        this.partitions = partitions;
    }
}
