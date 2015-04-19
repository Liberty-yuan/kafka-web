package me.bliss.kafka.web.component.model;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 4/11/15
 *          Exp $
 */
public class ZKBroker {

    private String host;

    private int port;

    private String version;

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

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
