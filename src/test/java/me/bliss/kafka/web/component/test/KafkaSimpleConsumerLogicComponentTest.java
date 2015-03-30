package me.bliss.kafka.web.component.test;

import me.bliss.kafka.web.component.KafkaSimpleConsumerLogicComponent;
import me.bliss.kafka.web.component.exception.SimpleConsumerLogicException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.WebApplicationContext;

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
public class KafkaSimpleConsumerLogicComponentTest {

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Autowired
    private KafkaSimpleConsumerLogicComponent kafkaPartitionsLeaderComponent;

    @Test
    public void testReadData() {
        try {
            kafkaPartitionsLeaderComponent.readData("zassets.ui.alipay.net", 9092, "build", 0, 10000);
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        }
    }

    public void setKafkaPartitionsLeaderComponent(
            KafkaSimpleConsumerLogicComponent kafkaPartitionsLeaderComponent) {
        this.kafkaPartitionsLeaderComponent = kafkaPartitionsLeaderComponent;
    }
}
