package com.rts.hbase.utils;

import java.io.InputStream;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * 从Hbase的配置文件hbase-site.xml读取连接hbase的信息，生成hbase的Configuration对象实例
 */
public class HbaseConnectionUtils {
//    private static final Logger LOG = LoggerFactory.getLogger(HbaseConnectionUtils.class);
    public static org.apache.hadoop.conf.Configuration getHHConfig() {
        Configuration conf = HBaseConfiguration.create();
        InputStream confResourceAsInputStream = conf.getConfResourceAsInputStream("hbase-site.xml");
        int available = 0;
        try {
            available = confResourceAsInputStream.available();
        } catch (Exception e) {
            //for debug purpose
//            LOG.debug("configuration files not found locally");
        } finally {
            IOUtils.closeQuietly(confResourceAsInputStream);
        }
        if (available == 0 ) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hbase-site.xml");
            conf.addResource("hdfs-site.xml");
        }
        return conf;
    }
    
    public static void main(String[] args) {
	}
}