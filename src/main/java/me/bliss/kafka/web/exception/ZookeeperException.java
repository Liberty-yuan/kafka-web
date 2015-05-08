package me.bliss.kafka.web.exception;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.exception, v 0.1 5/7/15
 *          Exp $
 */
public class ZookeeperException extends Exception{
    public ZookeeperException(String message) {
        super(message);
    }

    public ZookeeperException() {
        super();
    }

    public ZookeeperException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZookeeperException(Throwable cause) {
        super(cause);
    }

    protected ZookeeperException(String message, Throwable cause, boolean enableSuppression,
                                 boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static class ConnectionException extends ZookeeperException{
        public ConnectionException() {
            super();
        }
    }
}
