package com.tony.spark;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConfig {
//    private static final Logger LOG = LoggerFactory.getLogger(HbaseConfig.class);
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
//            IOUtils.closeQuietly(confResourceAsInputStream);
        }
        if (available == 0 ) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hbase-site.xml");
            conf.addResource("hdfs-site.xml");
        }
        return conf;
    }
}