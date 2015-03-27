package me.bliss.kafka.web.component.test;

import me.bliss.kafka.web.component.KafkaDataHandleComponent;
import org.junit.Assert;
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
 * @version $Id: me.bliss.kafka.web.service.test, v 0.1 3/19/15
 *          Exp $
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("file:src/main/webapp/WEB-INF/mvc-dispatcher-servlet.xml")
public class KafkaDataHandleComponentTest extends Assert{

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Autowired
    private KafkaDataHandleComponent kafkaDataHandleComponent;

    private String brokerHost = "zassets.ui.alipay.net";

    private int brokerPort = 9092;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    public void setKafkaDataHandleComponent(KafkaDataHandleComponent kafkaDataHandleComponent) {
        this.kafkaDataHandleComponent = kafkaDataHandleComponent;
    }

    @Test
    public void testGetEarliestOffset(){
        final long build = kafkaDataHandleComponent
                .getEarliestOffset(brokerHost, brokerPort, "build", 0);
        assertTrue(build > 0);
        System.out.println("earliest: "+build);
    }

    @Test
    public void testGetLastOffset(){
        final long build = kafkaDataHandleComponent
                .getLastOffset(brokerHost, brokerPort, "build", 0);
        assertTrue(build > 0);
        System.out.println("last: "+build);
    }

    @Test
    public void testReadData(){
        kafkaDataHandleComponent.readData(brokerHost, brokerPort,"build",0,1858,10);
    }

}
