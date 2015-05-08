package me.bliss.kafka.web.component.model;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.model, v 0.1 5/8/15
 *          Exp $
 */
public class ZK {

    private long sessionId;

    private int sessionTimeOut;

    private String host;

    private int port;

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public void setSessionTimeOut(int sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
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
