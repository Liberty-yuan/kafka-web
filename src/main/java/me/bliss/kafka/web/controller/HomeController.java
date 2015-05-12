package me.bliss.kafka.web.controller;

import me.bliss.kafka.web.component.model.Topic;
import me.bliss.kafka.web.service.BrokerService;
import me.bliss.kafka.web.service.TopicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.controller, v 0.1 5/7/15
 *          Exp $
 */
@Controller
public class HomeController {

    @Autowired
    private BrokerService brokerService;

    @Autowired
    private TopicService topicService;

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public String index(ModelMap model) {

        final Map<String, Object> result = topicService.getKafkaEnvDetail().getResult();
        model.put("topics", result.get("topics"));
        model.put("brokers", result.get("brokers"));
        model.put("zookeeper", result.get("zookeeper"));
        return "index";
    }

    @RequestMapping(value = "/topics", method = RequestMethod.GET)
    public String topics(ModelMap model) {
        final List<Topic> allTopics = topicService.getAllTopics();
        model.put("topics",allTopics);
        return "topics";
    }

    @RequestMapping(value = "/messages",method = RequestMethod.GET)
    public String messages(){
        return "messages";
    }

    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }
}
