package me.bliss.kafka.web.component.exception;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.exception, v 0.1 3/30/15
 *          Exp $
 */
public class SimpleConsumerLogicException extends Exception{

    public SimpleConsumerLogicException() {
        super();
    }

    public SimpleConsumerLogicException(String message) {
        super(message);
    }

    public SimpleConsumerLogicException(String message, Throwable cause) {
        super(message, cause);
    }

    public SimpleConsumerLogicException(Throwable cause) {
        super(cause);
    }

    protected SimpleConsumerLogicException(String message, Throwable cause,
                                           boolean enableSuppression,
                                           boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
