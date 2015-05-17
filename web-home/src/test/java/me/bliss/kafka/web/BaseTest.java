package me.bliss.kafka.web;

import org.junit.Test;

/**
 *
 *
 * @author lanjue
 * @version $Id: me.bliss.kafka.web, v 0.1 5/16/15
 *          Exp $
 */
public class BaseTest {

    @Test
    public void testName() throws Exception {
        String error = "13 : \"Error: 上传文件失败了\\n at /Users/lanjue/Documents/alipay_node/qingting-web/app/services/tfs.js:25:9\\n at";
        System.out.println(error.replace("\\n","<br>"));
    }
}
