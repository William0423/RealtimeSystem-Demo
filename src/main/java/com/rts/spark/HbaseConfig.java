package com.rts.spark;

import java.io.InputStream;
import java.sql.Date;
import java.text.SimpleDateFormat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

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
        SimpleDateFormat sdf = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
        String time = "1513248600000";
        String d = sdf.format(new Date(Long.parseLong(String.valueOf(time))));
        
        JSONObject json_data = new JSONObject();
        // parse raw data into a JSON object
        try {
			json_data.put("rowKey-em", "GE1/0/11");
	        json_data.put("family-datetime", d);
	        json_data.put("qualifier", "influx");
	        json_data.put("value", 38678);
	        json_data.put("Timestamp", 1513248600);
		} catch (JSONException e) {
			e.printStackTrace();
		}
        System.out.println(json_data.toString());
	}
}