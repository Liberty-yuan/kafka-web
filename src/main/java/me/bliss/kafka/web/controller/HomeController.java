package me.bliss.kafka.web.controller;

import me.bliss.kafka.web.component.model.PartitionMessage;
import me.bliss.kafka.web.component.model.Topic;
import me.bliss.kafka.web.component.model.TopicMessage;
import me.bliss.kafka.web.component.model.ZookeeperNode;
import me.bliss.kafka.web.service.TopicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
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
        model.put("topics", allTopics);
        return "topics";
    }

    @RequestMapping(value = "/nodes", method = RequestMethod.GET)
    @ResponseBody
    public List<ZookeeperNode> getAllNodes() {
        return topicService.getNodesTree().getResult();
    }

    @RequestMapping(value = "/messages", method = RequestMethod.GET)
    public String messages(ModelMap model) {
        final List<TopicMessage> topicMessages = topicService.getMessagesByReverse();
        for (TopicMessage topicMessage : topicMessages) {
            for (PartitionMessage partitionMessage : topicMessage.getPartitionMessages()) {
                partitionMessage.setMessages(partitionMessage.getMessages());
            }
        }
        model.put("topics", topicMessages);
        return "messages";
    }

    private List<String> brSplitList(List<String> messages) {
        final ArrayList<String> temp = new ArrayList<String>();
        for (String message : messages) {
            temp.add(message+"<br>");
        }
        return temp;
    }
}
