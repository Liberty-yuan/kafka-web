package me.bliss.kafka.web.service;

import me.bliss.kafka.web.component.SimpleConsumerComponent;
import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.component.model.ZK;
import me.bliss.kafka.web.component.model.ZKBroker;
import me.bliss.kafka.web.exception.ZookeeperException;
import me.bliss.kafka.web.result.ServiceResult;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 5/7/15
 *          Exp $
 */
public class BrokerService {

    @Autowired
    private SimpleConsumerComponent simpleConsumerComponent;

    @Autowired
    private ZookeeperComponent zookeeperComponent;

    public List<ZKBroker> getBrokers() {
        try {
            return zookeeperComponent.getBrokersList();
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void getKafkaEnvDetail() {
        final ServiceResult<Map<String, Object>> serviceResult = new ServiceResult<Map<String, Object>>();
        try {
            final ZK zkDetail = zookeeperComponent.getZKDetail();
            final List<ZKBroker> brokersList = zookeeperComponent.getBrokersList();
        } catch (ZookeeperException e) {
            e.printStackTrace();
        }
    }

    public void setZookeeperComponent(ZookeeperComponent zookeeperComponent) {
        this.zookeeperComponent = zookeeperComponent;
    }

    public void setSimpleConsumerComponent(SimpleConsumerComponent simpleConsumerComponent) {
        this.simpleConsumerComponent = simpleConsumerComponent;
    }
}
