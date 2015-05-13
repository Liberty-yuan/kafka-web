package me.bliss.kafka.web.component.test;

import junit.framework.Assert;
import me.bliss.kafka.web.component.SimpleConsumerComponent;
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
@ContextConfiguration(locations = {"file:src/main/webapp/WEB-INF/spring/application.xml"})
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
    private SimpleConsumerComponent simpleConsumerComponent;

    private String host = "zassets.ui.alipay.net";

    private int port = 9092;

    private String topic = "build";

    private int partition = 0;

    @Test
    public void testGetData() throws Exception {
        final List<String> readData = simpleConsumerComponent
                .readData(host, port, "qt_error", 1, 1000);
        System.out.println(ArrayUtils.toString(readData));
    }

    @Test
    public void testGetLastoffset() throws Exception {
        final long offset = simpleConsumerComponent.getLastOffset(host, port, "qt_error", 0);
        System.out.println(offset);
    }

    public void setSimpleConsumerComponent(SimpleConsumerComponent simpleConsumerComponent) {
        this.simpleConsumerComponent = simpleConsumerComponent;
    }
}
