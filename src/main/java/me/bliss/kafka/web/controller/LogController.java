package me.bliss.kafka.web.controller;

import kafka.log.Log;
import me.bliss.kafka.web.model.LogRecord;
import me.bliss.kafka.web.result.FacadeResult;
import me.bliss.kafka.web.service.HandleLogSegmentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

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

    private String logRootPath = "/tmp/kafka-logs";

    @RequestMapping(value = "/logs", method = RequestMethod.GET)
    public String getFilesList(ModelMap modelMap) {
        final File[] files = new File(logRootPath).listFiles();
        final ArrayList<String> filenames = new ArrayList<String>();
        for (File file : files){
            if (!file.getName().startsWith("_") && !file.getName().startsWith(".")) {
                filenames.add(file.getName());
            }
        }
        modelMap.put("filenames",filenames);

        return "logs";
    }

    @RequestMapping(value = "/logs/get/{name}",method = RequestMethod.GET)
    @ResponseBody
    public FacadeResult getFileContent(@PathVariable String name){
        final FacadeResult<List<LogRecord>> facadeResult = new FacadeResult<List<LogRecord>>();
        final File topicDir = new File(logRootPath + "/" + name);
        final ArrayList<LogRecord> allRecords = new ArrayList<LogRecord>();
        if (topicDir.isDirectory()){
            for (File file : topicDir.listFiles()){
                if (file.getName().endsWith(Log.LogFileSuffix())) {
                    final List<LogRecord> logRecords = handleLogSegmentService.dumpLog(file);
                    allRecords.addAll(logRecords);
                }
            }
        }

        facadeResult.setSuccess(true);
        facadeResult.setResult(allRecords);
        return facadeResult;
    }

    public void setHandleLogSegmentService(HandleLogSegmentService handleLogSegmentService) {
        this.handleLogSegmentService = handleLogSegmentService;
    }
}

