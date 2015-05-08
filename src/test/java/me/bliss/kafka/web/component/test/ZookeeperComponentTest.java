package me.bliss.kafka.web.component.test;

import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.exception.ZookeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
 * @version $Id: me.bliss.kafka.web.service.test, v 0.1 3/3/15
 *          Exp $
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(locations = {"file:src/main/webapp/WEB-INF/spring/application.xml"})
public class ZookeeperComponentTest extends Assert{

    @SuppressWarnings("SpringJavaAutowiringInspection")
    @Autowired
    protected WebApplicationContext webApplicationContext;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = webAppContextSetup(this.webApplicationContext).build();
    }

    @Autowired
    private ZookeeperComponent zookeeperComponent;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testCreate() throws Exception {
        expectedException.expect(ZookeeperException.class);
        expectedException.expectMessage("创建节点失败");
        zookeeperComponent.create("/lanjue", "第一条测试数据");
    }

    @Test
    public void testGetData() throws Exception {
        final String data = zookeeperComponent.getData("/lanjue");
        assertEquals("第一条测试数据",data);
    }

    @Test
    public void testGetChildren() throws Exception {
        final List<String> children = zookeeperComponent.getChildren("/lanjue");
        assertNotNull(children);
        assertTrue(children.size() == 0);
    }

    @Test
    public void testGetZKDetail() throws Exception {
        zookeeperComponent.getZKDetail();
    }

    public void setZookeeperComponent(ZookeeperComponent zookeeperComponent) {
        this.zookeeperComponent = zookeeperComponent;
    }
}
