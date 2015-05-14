package me.bliss.kafka.service;

import kafka.log.Log;
import me.bliss.kafka.core.component.KafkaLogSegmentComponent;
import me.bliss.kafka.model.LogRecord;
import org.apache.commons.lang.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.util.*;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web.service, v 0.1 3/19/15
 *          Exp $
 */
public class KafkaLogService {

    @Value("${logpath}")
    private String logRootPath;

    @Autowired
    private KafkaLogSegmentComponent handleLogSegmentComponent;

    public Map<String, List<String>> getAllTopicFilenames() {
        final String[] logPaths = logRootPath.split(",");
        final File rootPathDir = new File(logPaths[0]);
        if (!rootPathDir.exists()) {
            throw new RuntimeException("kafka logs directory not exists");
        }
        final File[] files = rootPathDir.listFiles();
        final HashMap<String, List<String>> filenames = new HashMap<String, List<String>>();
        for (File file : files) {
            if (file.isFile()) {
                continue;
            }
            String topic = file.getName().substring(0, file.getName().lastIndexOf("-"));
            if (filenames.containsKey(topic)) {
                final List<String> topicRepalications = filenames.get(topic);
                topicRepalications.add(file.getName());
                filenames.remove(topic);
                filenames.put(topic, topicRepalications);
            } else {
                final List<String> topicRepalications = new ArrayList<String>();
                topicRepalications.add(file.getName());
                filenames.put(topic, topicRepalications);
            }
        }

        System.out.println(ArrayUtils.toString(filenames));
        return filenames;

    }

    public List<LogRecord> getFileContent(String topicParatition) {
        final String[] logPaths = logRootPath.split(",");
        final ArrayList<LogRecord> allRecords = new ArrayList<LogRecord>();
        for (String logPath : logPaths){
            final File topicDir = new File(logPath + "/" + topicParatition);
            if (topicDir.isDirectory()) {
                for (File file : topicDir.listFiles()) {
                    if (file.getName().endsWith(Log.LogFileSuffix())) {
                        final List<LogRecord> logRecords;
                        logRecords = handleLogSegmentComponent.dumpLog(file);
                        allRecords.addAll(logRecords);

                    }
                }
            }
        }
        return allRecords;
    }

    public List<LogRecord> getReverseFileContent(String topicParatition) {
        final List<LogRecord> allRecords = getFileContent(topicParatition);
        Collections.reverse(allRecords);
        System.out.println(allRecords);
        return allRecords;
    }

    public List<LogRecord> getFileContent(String topicParatition, int startPos, int messageCount) {
        final File topicDir = new File(logRootPath + "/" + topicParatition);
        final ArrayList<LogRecord> allRecords = new ArrayList<LogRecord>();
        if (topicDir.isDirectory()) {
            for (File file : topicDir.listFiles()) {
                if (file.getName().endsWith(Log.LogFileSuffix())) {
                    final List<LogRecord> logRecords;
                    logRecords = handleLogSegmentComponent.dumpLog(file, startPos, messageCount);
                    allRecords.addAll(logRecords);

                }
            }
        }
        return allRecords;
    }

    public List<LogRecord> getReverseFileContent(String topicParatition, int startPos,
                                                 int messageCount) {
        final List<LogRecord> allRecords = getFileContent(topicParatition);
        Collections.reverse(allRecords);
        final List<LogRecord> subRecords = allRecords.subList(startPos, startPos + messageCount);
        System.out.println(subRecords);
        return subRecords;
    }

    public void setLogRootPath(String logRootPath) {
        this.logRootPath = logRootPath;
    }

    public String getLogRootPath() {
        return logRootPath;
    }
}
