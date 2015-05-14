package me.bliss.kafka.model;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.model, v 0.1 5/12/15
 *          Exp $
 */
public class ZookeeperNode {

    private String path;

    private String data;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override public String toString() {
        return path + ":" + data;
    }
}
