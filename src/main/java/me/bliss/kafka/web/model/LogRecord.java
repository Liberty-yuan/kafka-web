package me.bliss.kafka.web.model;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.model, v 0.1 3/2/15
 *          Exp $
 */
public class LogRecord {

    private long offset;

    private long position;

    private boolean isvalid;

    private int contentSize;

    private String compresscodec;

    private String content;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
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

    public String getCompresscodec() {
        return compresscodec;
    }

    public void setCompresscodec(String compresscodec) {
        this.compresscodec = compresscodec;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override public String toString() {
        return new StringBuffer("[")
                .append(" offset: ").append(offset)
                .append(" position: ").append(position)
                .append(" isvalid: ").append(isvalid)
                .append(" encodc: ").append(compresscodec)
                .append(" contentSize: ").append(contentSize)
                .append(" content: ").append(content).toString();

    }
}
