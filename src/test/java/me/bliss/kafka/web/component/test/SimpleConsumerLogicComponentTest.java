package me.bliss.kafka.web.component.test;

import kafka.javaapi.consumer.SimpleConsumer;
import me.bliss.kafka.web.component.SimpleConsumerLogicComponent;
import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
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
@ContextConfiguration("file:src/main/webapp/WEB-INF/mvc-dispatcher-servlet.xml")
public class SimpleConsumerLogicComponentTest {

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Autowired
    private SimpleConsumerLogicComponent kafkaPartitionsLeaderComponent;

    @Test
    public void testReadData() {
        try {
            kafkaPartitionsLeaderComponent.readData("zassets.ui.alipay.net", 9092, "build", 0, 2);
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void testReadDataForPage(){
        try {
            final SimpleConsumer simpleConsumer = kafkaPartitionsLeaderComponent
                    .getLeaderSimpleConsumer("zassets.ui.alipay.net", 9092, "build", 0);
            kafkaPartitionsLeaderComponent.readDataForPage(simpleConsumer,"build",0,1800,2);
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void setKafkaPartitionsLeaderComponent(
            SimpleConsumerLogicComponent kafkaPartitionsLeaderComponent) {
        this.kafkaPartitionsLeaderComponent = kafkaPartitionsLeaderComponent;
    }
}
