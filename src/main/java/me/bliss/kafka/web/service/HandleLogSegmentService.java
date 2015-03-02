package me.bliss.kafka.web.service;

import kafka.log.FileMessageSet;
import kafka.log.Log;
import kafka.log.OffsetIndex;
import kafka.log.OffsetPosition;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.message.MessageSet;
import kafka.message.NoCompressionCodec;
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





    public List<String> dumpLog(File file,
                                HashMap<String, Map<Long, Long>> nonConsecutivePairsForLogFilesMap) {
        final ArrayList<String> result = new ArrayList<String>();
        final FileMessageSet fileMessage = new FileMessageSet(file, false);
        final FileMessageSet fileMessageSet = fileMessage.read(0, MessageSet.entrySize(fileMessage.iterator().next().message()) * 4);
        //fileMessageSet.read(9,10);
        long lastOffset = 0l;
        long validBytes = 0l;
        final Iterator<MessageAndOffset> iterator = fileMessageSet.iterator();
        while (iterator.hasNext()) {
            final MessageAndOffset messageAndOffset = iterator.next();
            final StringBuffer record = new StringBuffer();
            Message message = messageAndOffset.message();
            if (message.compressionCodec().codec() == NoCompressionCodec.codec()
                && messageAndOffset.offset() != lastOffset + 1) {
                //不连续的消息
                Map<Long, Long> nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap
                                                                 .get(file.getAbsolutePath())
                                                         == null ?
                        new HashMap<Long, Long>() :
                        nonConsecutivePairsForLogFilesMap.get(file.getAbsolutePath());
                nonConsecutivePairsSeq.put(lastOffset, messageAndOffset.offset());
                nonConsecutivePairsForLogFilesMap
                        .put(file.getAbsolutePath(), nonConsecutivePairsSeq);
            }
            lastOffset = messageAndOffset.offset();

            record.append("offset: ").append(messageAndOffset.offset())
                    .append(" position: ").append(validBytes)
                    .append(" isvalid: ").append(message.isValid())
                    .append(" payloadsize: ").append(message.payloadSize())
                    .append(" compresscodec: ").append(message.compressionCodec());

            if (message.hasKey()) {
                System.out.print(" keySize: " + message.keySize());
            }

            String payload = messageAndOffset.message().isNull() ?
                    null :
                    decodeByteBuffer(messageAndOffset.message().payload());
            record.append("payload").append(payload);
            result.add(record.toString());
            validBytes += MessageSet.entrySize(messageAndOffset.message());
        }
        final long trailingBytes = fileMessageSet.sizeInBytes() - validBytes;
        if (trailingBytes > 0) {
            System.out.format("Found %d invalid bytes at the end of %s ", trailingBytes,
                    file.getName());
        }
        return result;
    }

    private void readLog(){

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
        final Charset charset = Charset.forName("UTF-8");
        //byteBuffer.flip();
        final char[] array = charset.decode(byteBuffer).array();
        return new String(array);
    }

}
