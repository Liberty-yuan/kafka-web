package me.bliss.kafka.web.controller;

import me.bliss.kafka.web.service.HandleLogSegmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private HandleLogSegmentService handleLogSegmentService;

    @RequestMapping(value = "/logs", method = RequestMethod.GET)
    public String getFileContent(ModelMap modelMap) {
        final List<String> logLists = handleLogSegmentService
                .dumpLog(new File("/tmp/kafka-logs/build-0/00000000000000000000.log"),
                        new HashMap<String, Map<Long, Long>>());
        modelMap.put("logs", logLists);
        return "logs";
    }

    public void setHandleLogSegmentService(HandleLogSegmentService handleLogSegmentService) {
        this.handleLogSegmentService = handleLogSegmentService;
    }
}

