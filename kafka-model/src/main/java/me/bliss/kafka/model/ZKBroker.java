package me.bliss.kafka.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 4/11/15
 *          Exp $
 */
public class ZKBroker {

    private int id;

    private String host;

    private int port;

    private String version;

    private Date createDate;

    private long timestamp;

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

    public Date getCreateDate() {
        if (createDate == null ){
            SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );
            try {
                return format.parse(format.format(timestamp));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return createDate;
    }

    public void setCreateDate(Date createDate) {
        this.createDate = createDate;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
