package me.bliss.kafka.web.component.model;

import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.model, v 0.1 5/12/15
 *          Exp $
 */
public class TopicMessage {

    private String name;

    private List<PartitionMessage> partitionMessages;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<PartitionMessage> getPartitionMessages() {
        return partitionMessages;
    }

    public void setPartitionMessages(List<PartitionMessage> partitionMessages) {
        this.partitionMessages = partitionMessages;
    }
}
