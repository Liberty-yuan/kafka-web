package me.bliss.kafka.web.service.test;

import me.bliss.kafka.web.service.ZookeeperService;
import org.junit.Test;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service.test, v 0.1 3/3/15
 *          Exp $
 */
public class ZookeeperServiceTest {


    @Test
    public void testGetBroker(){
        ZookeeperService.getBrokers();
    }

    @Test
    public void testGetTopicDetail(){
        ZookeeperService.getTopicDetail("build");
    }
}
