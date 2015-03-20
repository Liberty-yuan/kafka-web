package me.bliss.kafka.web.controller;

import me.bliss.kafka.web.model.LogRecord;
import me.bliss.kafka.web.result.FacadeResult;
import me.bliss.kafka.web.service.KafkaLogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.controller, v 0.1 3/2/15
 *          Exp $
 */

@Controller
public class LogController {

    @Autowired
    private KafkaLogService kafkaLogService;

    @Value("${logpath}")
    private String logRootPath;

    @RequestMapping(value = "/config", method = RequestMethod.GET)

    @ResponseBody
    public String getConfig() {
        final Properties properties = new Properties();
        //        try {
        //            properties.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
        return logRootPath;
    }

    @RequestMapping(value = "/logs", method = RequestMethod.GET)
    public String getFilesList(ModelMap modelMap) {

        final Map<String, List<String>> allTopicFilenames = kafkaLogService.getAllTopicFilenames();
        modelMap.put("topicFilenames", allTopicFilenames);

        return "logs";
    }

    @RequestMapping(value = "/logs/get/{name}", method = RequestMethod.GET)
    @ResponseBody
    public FacadeResult getFileContent(@PathVariable String name) {
        final FacadeResult<List<LogRecord>> facadeResult = new FacadeResult<List<LogRecord>>();
        final List<LogRecord> allRecords = kafkaLogService.getReverseFileContent(name);
        facadeResult.setSuccess(true);
        facadeResult.setResult(allRecords);
        return facadeResult;
    }

    public KafkaLogService getKafkaLogService() {
        return kafkaLogService;
    }

    public void setLogRootPath(String logRootPath) {
        this.logRootPath = logRootPath;
    }
}

