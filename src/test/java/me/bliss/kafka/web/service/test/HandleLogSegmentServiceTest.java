package me.bliss.kafka.web.service.test;

import junit.framework.Assert;
import me.bliss.kafka.web.component.KafkaLogSegmentComponent;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service.test, v 0.1 3/6/15
 *          Exp $
 */
public class HandleLogSegmentServiceTest extends Assert{

    private static KafkaLogSegmentComponent handleLogSegmentComponent;

    @BeforeClass
    public static void before() {
        handleLogSegmentComponent = new KafkaLogSegmentComponent();
    }

    @Test
    public void testDumpIndex() {
        final List<String> logRecords = handleLogSegmentComponent.dumpIndex(
                new File("/tmp/kafka-logs/build-0/00000000000000000000.index"), false, 100);
        System.out.println(logRecords);
    }

}
