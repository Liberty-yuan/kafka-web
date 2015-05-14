package me.bliss.kafka.model;

import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.model, v 0.1 5/12/15
 *          Exp $
 */
public class PartitionMessage {

    private int id;

    private List<String> messages;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<String> getMessages() {
        return messages;
    }

    public void setMessages(List<String> messages) {
        this.messages = messages;
    }
}
