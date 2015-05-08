package me.bliss.kafka.web.component.test;

import junit.framework.Assert;
import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.SimpleConsumerComponent;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

import java.io.UnsupportedEncodingException;
import java.util.List;

import static org.springframework.test.web.servlet.setup.MockMvcBuilders.webAppContextSetup;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.component.test, v 0.1 3/30/15
 *          Exp $
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(
        "file:../../../../../../../../main/webapp/WEB-INF/spring/mvc-dispatcher-servlet.xml")
public class SimpleConsumerLogicComponentTest extends Assert{

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Autowired
    private SimpleConsumerComponent kafkaPartitionsLeaderComponent;

    private String host = "zassets.ui.alipay.net";

    private int port = 9092;

    private String topic = "build";

    private int partition = 0;

    @Test
    public void testReadData() {
        try {
            final List<String> readData = kafkaPartitionsLeaderComponent
                    .readData(host, port, topic, partition, 2);
            System.out.println(ArrayUtils.toString(readData));
            assertNotNull(readData);
            assertTrue(readData.size() > 0);
        } catch (SimpleConsumerLogicException e) {
            assertNotNull(e);
        } catch (UnsupportedEncodingException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testReadDataForPage() {
        try {
            final SimpleConsumer simpleConsumer = kafkaPartitionsLeaderComponent
                    .getLeaderSimpleConsumer(host, port, topic, partition);
            final List<String> readDataForPage = kafkaPartitionsLeaderComponent
                    .readDataForPage(simpleConsumer, topic, partition, 1800, 2);
            assertNotNull(readDataForPage);
            assertTrue(readDataForPage.size() == 2);
        } catch (SimpleConsumerLogicException e) {
            assertNotNull(e);
        } catch (UnsupportedEncodingException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testGetEarliestOffset() {
        try {
            final long earliestOffset = kafkaPartitionsLeaderComponent
                    .getEarliestOffset(host, port, topic, partition);
            assertTrue(earliestOffset >= 0);
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        }
    }

    public void setKafkaPartitionsLeaderComponent(
            SimpleConsumerComponent kafkaPartitionsLeaderComponent) {
        this.kafkaPartitionsLeaderComponent = kafkaPartitionsLeaderComponent;
    }
}
