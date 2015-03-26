package me.bliss.kafka.web.component.model;

import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model.consumer, v 0.1 3/26/15
 *          Exp $
 */
public class Partitions {

    private int id;

    private Replication leader;

    private List<Replication> replicas;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Replication getLeader() {
        return leader;
    }

    public void setLeader(Replication leader) {
        this.leader = leader;
    }

    public List<Replication> getReplicas() {
        return replicas;
    }

    public void setReplicas(List<Replication> replicas) {
        this.replicas = replicas;
    }
}
