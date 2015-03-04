package me.bliss.kafka.web.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 3/4/15
 *          Exp $
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Broker {

    private String jmxPort;

    private long timestamp;

    private String host;

    private String version;

    private int port;

    public String getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(String jmxPort) {
        this.jmxPort = jmxPort;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
