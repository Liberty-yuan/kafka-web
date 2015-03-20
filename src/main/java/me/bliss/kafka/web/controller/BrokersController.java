package me.bliss.kafka.web.controller;

import me.bliss.kafka.web.component.ZookeeperComponent;
import me.bliss.kafka.web.model.Broker;
import me.bliss.kafka.web.model.Topic;
import me.bliss.kafka.web.result.FacadeResult;
import me.bliss.kafka.web.result.ServiceResult;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.controller, v 0.1 3/3/15
 *          Exp $
 */
@Controller
public class BrokersController {

    @RequestMapping(value = "/brokers",method = RequestMethod.GET)
    public String detail(ModelMap modelMap){
        final ServiceResult<Map<String,Object>> serviceResult = ZookeeperComponent.getBrokers();
        modelMap.put("brokers",serviceResult.getResult().get("ids"));
        modelMap.put("topics",serviceResult.getResult().get("topics"));
        return "brokers";
    }

    @RequestMapping(value = "/topics",method = RequestMethod.GET)
    public String topic(ModelMap modelMap){
        final ServiceResult<Map<String,Object>> serviceResult = ZookeeperComponent.getBrokers();
        modelMap.put("topics",serviceResult.getResult().get("topics"));
        return "topics";
    }

    @RequestMapping(value = "/topics/get/{topic}",method = RequestMethod.GET)
    @ResponseBody
    public FacadeResult getTopicDetail(@PathVariable String topic){
        final FacadeResult<Map<String, Object>> facadeResult = new FacadeResult<Map<String, Object>>();
        final ServiceResult<Topic> topicsDetail = ZookeeperComponent.getTopicDetail(topic);
        final ServiceResult<Map<String, Broker>> brokersDetail = ZookeeperComponent
                .getBrokersDetail();
        final HashMap<String, Object> result = new HashMap<String, Object>();
        if (topicsDetail.isSuccess() && brokersDetail.isSuccess()){
            facadeResult.setSuccess(true);
            result.put("topics",topicsDetail.getResult());
            result.put("brokers",brokersDetail.getResult());
            facadeResult.setResult(result);
            return facadeResult;
        }
        facadeResult.setSuccess(false);
        facadeResult.setErrorMsg("interface error!!!");
        return facadeResult;
    }
}
