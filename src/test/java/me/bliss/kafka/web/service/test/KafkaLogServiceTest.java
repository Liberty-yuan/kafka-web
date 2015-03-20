package me.bliss.kafka.web.service.test;

import me.bliss.kafka.web.service.KafkaLogService;
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
public class KafkaLogServiceTest extends Assert{

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Autowired
    private KafkaLogService kafkaLogService;

    @Test
    public void testGetAllTopicLogs() {
        kafkaLogService.getAllTopicFilenames();
    }

    @Test
    public void testGetFileContent() {
        kafkaLogService.getFileContent("build-0");
    }

    @Test
    public void testGetFileContentByOps() {
        kafkaLogService.getFileContent("build-0",1,1);
    }

    @Test
    public void testGetReverseFileContent(){
        kafkaLogService.getReverseFileContent("build-0");
    }

    @Test
    public void testGetReverseFileContentOps(){
        kafkaLogService.getReverseFileContent("build-0",0,1);
    }


}
