package me.bliss.kafka.web.service;

import kafka.log.*;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.message.NoCompressionCodec;
import me.bliss.kafka.web.constant.ServiceContants;
import me.bliss.kafka.web.model.LogRecord;
import scala.collection.Iterator;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.service, v 0.1 3/2/15
 *          Exp $
 */
public class HandleLogSegmentService {

    public List<LogRecord> dumpLog(File file,int startPos,int messageCount) {
        final FileMessageSet fileMessage = new FileMessageSet(file, false);
        final FileMessageSet fileMessageSet = fileMessage.read(startPos, MessageSet.entrySize(fileMessage.iterator().next().message()) * messageCount);

        return readLog(file,fileMessageSet);
    }

    public List<LogRecord> dumpLog(File file){
        final FileMessageSet fileMessageSet = new FileMessageSet(file, false);
        return readLog(file,fileMessageSet);
    }

    private List<LogRecord> readLog(File file,FileMessageSet fileMessageSet){
        final Iterator<MessageAndOffset> iterator = fileMessageSet.iterator();
        final ArrayList<LogRecord> result = new ArrayList<LogRecord>();
        long lastOffset = 0l;
        long validBytes = 0l;
        while (iterator.hasNext()) {
            final MessageAndOffset messageAndOffset = iterator.next();
            final LogRecord logRecord = new LogRecord();
            Message message = messageAndOffset.message();

            getNonConsecutivePairs(file,messageAndOffset,lastOffset);

            lastOffset = messageAndOffset.offset();
            if (message.hasKey()) {
                System.out.print(" keySize: " + message.keySize());
            }

            String payload = messageAndOffset.message().isNull() ?
                    null :
                    decodeByteBuffer(messageAndOffset.message().payload());

            logRecord.setOffset(messageAndOffset.offset());
            logRecord.setPosition(validBytes);
            logRecord.setIsvalid(message.isValid());
            logRecord.setContentSize(message.payloadSize());
            logRecord.setCompresscodec(message.compressionCodec());
            logRecord.setContent(payload);

            result.add(logRecord);
            validBytes += MessageSet.entrySize(messageAndOffset.message());
        }
        final long trailingBytes = fileMessageSet.sizeInBytes() - validBytes;
        if (trailingBytes > 0) {
            System.out.format("Found %d invalid bytes at the end of %s ", trailingBytes,
                    file.getName());
        }
        return result;
    }


    public List<String> dumpIndex(File file, boolean verifyOnly,
                                  Map<String, Map<Long, Long>> misMatchesForIndexFilesMap,
                                  int maxMessageSize) {
        final List<String> result = new ArrayList<String>();
        long startOffset = Long.parseLong(file.getName().split("\\.")[0]);
        String logFileName = file.getAbsolutePath().split("\\.")[0] + Log.LogFileSuffix();
        File logFile = new File(logFileName);
        final FileMessageSet messageSet = new FileMessageSet(logFile, false);
        final OffsetIndex index = new OffsetIndex(file, startOffset, maxMessageSize);
        for (int i = 0; i < index.entries(); i++) {
            final OffsetPosition entry = index.entry(i);
            final FileMessageSet partialFileMessageSet = messageSet
                    .read(entry.position(), maxMessageSize);
            final MessageAndOffset messageAndOffset = (MessageAndOffset) partialFileMessageSet
                    .head();
            if (messageAndOffset.offset() != entry.offset() + index.baseOffset()) {
                final Map<Long, Long> misMatchesSeq =
                        misMatchesForIndexFilesMap.get(file.getAbsolutePath()) == null ?
                                misMatchesForIndexFilesMap.get(file.getAbsolutePath()) :
                                new HashMap<Long, Long>();
                misMatchesSeq.put(entry.offset() + index.baseOffset(), messageAndOffset.offset());
                misMatchesForIndexFilesMap.put(file.getAbsolutePath(), misMatchesSeq);
            }

            if (entry.offset() == 0 && i > 0) {
                return null;
            }
            if (!verifyOnly) {
                result.add(String.format("offset: %d pasition: %d",
                        entry.offset() + index.baseOffset(), entry.position()));
            }
        }
        return result;
    }
    private String decodeByteBuffer(ByteBuffer byteBuffer) {
        final Charset charset = Charset.forName(ServiceContants.MESSAGE_ENCODE);
        //byteBuffer.flip();
        final char[] array = charset.decode(byteBuffer).array();
        return new String(array);
    }

    private Map<String,Map<Long,Long>> getNonConsecutivePairs(File file,MessageAndOffset messageAndOffset,long lastOffset){
        final HashMap<String, Map<Long, Long>> nonConsecutivePairsForLogFilesMap = new HashMap<String, Map<Long, Long>>();
        if (messageAndOffset.message().compressionCodec().codec() == NoCompressionCodec.codec()
            && messageAndOffset.offset() != lastOffset + 1) {
            //不连续的消息
            Map<Long, Long> nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.get(file.getAbsolutePath())== null ?
                    new HashMap<Long, Long>() :
                    nonConsecutivePairsForLogFilesMap.get(file.getAbsolutePath());
            nonConsecutivePairsSeq.put(lastOffset, messageAndOffset.offset());
            nonConsecutivePairsForLogFilesMap
                    .put(file.getAbsolutePath(), nonConsecutivePairsSeq);
        }

        return nonConsecutivePairsForLogFilesMap;
    }

}
