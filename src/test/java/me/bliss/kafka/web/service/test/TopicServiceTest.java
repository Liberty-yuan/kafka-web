package me.bliss.kafka.web.service.test;

import me.bliss.kafka.web.exception.SimpleConsumerLogicException;
import me.bliss.kafka.web.service.TopicService;
import org.apache.zookeeper.KeeperException;
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
 * @version $Id: me.bliss.kafka.web.service.test, v 0.1 4/11/15
 *          Exp $
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration("file:src/main/webapp/WEB-INF/mvc-dispatcher-servlet.xml")
public class TopicServiceTest {

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Autowired
    private TopicService topicService;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Test
    public void testGetAllTopics() {
        try {
            topicService.getAllTopics();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SimpleConsumerLogicException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetMessage() {
        topicService.getMessage();

    }

    public void setTopicService(TopicService topicService) {
        this.topicService = topicService;
    }
}
