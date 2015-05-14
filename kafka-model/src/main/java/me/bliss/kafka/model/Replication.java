package me.bliss.kafka.model;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model.consumer, v 0.1 3/26/15
 *          Exp $
 */
public class Replication {

    private int id;

    private String host;

    private int port;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
