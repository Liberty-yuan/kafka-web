package me.bliss.kafka.web.model;

import kafka.message.CompressionCodec;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 3/2/15
 *          Exp $
 */
public class LogRecord {

    private String offset;

    private int position;

    private boolean isvalid;

    private int contentSize;

    private CompressionCodec compresscodec;

    private String content;

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public boolean isIsvalid() {
        return isvalid;
    }

    public void setIsvalid(boolean isvalid) {
        this.isvalid = isvalid;
    }

    public int getContentSize() {
        return contentSize;
    }

    public void setContentSize(int contentSize) {
        this.contentSize = contentSize;
    }

    public CompressionCodec getCompresscodec() {
        return compresscodec;
    }

    public void setCompresscodec(CompressionCodec compresscodec) {
        this.compresscodec = compresscodec;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
