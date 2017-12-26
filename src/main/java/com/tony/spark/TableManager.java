package com.tony.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class TableManager {

	  private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
	  private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

	  public static void createSchemaTables(Configuration config) throws IOException {
		    try (Connection connection = ConnectionFactory.createConnection(config);
		         Admin admin = connection.getAdmin()) {

		      HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		      HColumnDescriptor hcd = new HColumnDescriptor(CF_DEFAULT);
		      hcd.setCompressionType(Algorithm.NONE);
		      table.addFamily(hcd);
		      
		      
		      System.out.print("Creating table. ");
		      createOrOverwrite(admin, table);
		      System.out.println(" Done.");
		    }
		  }
	  
	  /**
	   * 创建表
	   * @param admin
	   * @param table
	   * @throws IOException
	   */
	  public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
	    if (admin.tableExists(table.getTableName())) {
	      admin.disableTable(table.getTableName());
	      admin.deleteTable(table.getTableName());
	    }
	    admin.createTable(table);
	    
	    HTableDescriptor[] tableDescriptor = admin.listTables();
	    for(int i=0;i<tableDescriptor.length;i++) {
	    	System.out.println(tableDescriptor[i].getNameAsString());
	    }
//	    Scan scan = new Scan();
//        ResultScanner scanner = tablet.getScanner(scan);
//		for (Result scannerResult : scanner) {
//		    System.out.println("Scan: " + scannerResult);
//		}
	  }
	  
//	  public static void findSchemaTables(Admin admin) throws IOException {
//		  HTableDescriptor[] tables = admin.listTables();
//		  if (tables.length != 1&& )
//	  }
	  
	  public static void scanSchema(Configuration config) throws IOException {
		  Connection connection = ConnectionFactory.createConnection(config);
		  Admin admin = connection.getAdmin();
		  
		  HTableDescriptor[] tableDescriptor = admin.listTables();
		    for(int i=0;i<tableDescriptor.length;i++) {
		    	System.out.println(tableDescriptor[i].getNameAsString());
		    }
	  }

	  public static void modifySchema (Configuration config) throws IOException {
	    try (Connection connection = ConnectionFactory.createConnection(config);
	         Admin admin = connection.getAdmin()) {

	      TableName tableName = TableName.valueOf(TABLE_NAME);
	      if (!admin.tableExists(tableName)) {
	        System.out.println("Table does not exist.");
	        System.exit(-1);
	      }

	      HTableDescriptor table = admin.getTableDescriptor(tableName);

	      // Update existing table
	      HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
	      newColumn.setCompactionCompressionType(Algorithm.GZ);
	      newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
	      admin.addColumn(tableName, newColumn);

	      // Update existing column family
	      HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
	      existingColumn.setCompactionCompressionType(Algorithm.GZ);
	      existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
	      table.modifyFamily(existingColumn);
	      admin.modifyTable(tableName, table);

	      // Disable an existing table
//	      admin.disableTable(tableName);
//
//	      // Delete an existing column family
//	      admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));
//
//	      // Delete a table (Need to be disabled first)
//	      admin.deleteTable(tableName);
	    }
	  }
	  
	  public static void putData(Configuration config) throws IOException {
		  // 获得连接：
		  Connection connection = ConnectionFactory.createConnection(config);
		  
//		  HTableDescriptor[] tableDescriptor = admin.listTables();
		  TableName tablename = TableName.valueOf("member");
//		  HTableDescriptor table = admin.getTableDescriptor(tablename); //表结构层面的的table对象
		  Table table = connection.getTable(tablename); // 表数据层面的table对象

//		  Put put = new Put(Bytes.toBytes(rowKey));
//		  put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
		  // 列由family:qualifier 两部分组成
//		  System.out.println("Add data successfully!rowKey:"+rowKey+", column:"+family+":"+column+", cell:"+value);
		  Put put = new Put(Bytes.toBytes("debugo"));
		  put.addColumn(Bytes.toBytes("id"), Bytes.toBytes(""), Bytes.toBytes("11"));
		  put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("coutry"), Bytes.toBytes("China"));
		  put.addColumn(Bytes.toBytes("address"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
		  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("27"));
		  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("birthday"), Bytes.toBytes("1987-04-04"));
		  table.put(put);
		  table.close();
	  }
	  
	public static void getData(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);

		Get get = new Get(Bytes.toBytes("debugo"));
		Result result = table.get(get);
		String str = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
		System.out.println(str);
		table.close();
	}
	
	public static void deleteData(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);

		Delete delete = new Delete(Bytes.toBytes("debugo")); 
		table.delete(delete);  
		table.close();
	}
	
	public static void scanData(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);
		Scan scan = new Scan(); 
		ResultScanner resultScaner = table.getScanner(scan);
		
//		for (Result result : resultScaner) {  
////	        String str = Bytes.toString(result.getValue(Bytes.toBytes("debugo")));  
//	        System.out.println(result);  
//	    }
		
		for (Result result : resultScaner) { 
			for (KeyValue kv : result.list()) {
				System.out.println("row:" + Bytes.toString(kv.getRow()));
	            System.out.println("family:" + Bytes.toString(kv.getFamily()));
	            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
	            System.out.println("value:" + Bytes.toString(kv.getValue()));
	            System.out.println("timestamp:" + kv.getTimestamp());
	            System.out.println("-------------------------------------------");
			}
			
	    }  
	}

	  public static void main(String... args) throws IOException {
//	    Configuration config = HBaseConfiguration.create();
//	    //Add any necessary configuration files (hbase-site.xml, core-site.xml)
//	    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		  
//	        config.addResource(new Path("E:\\workplaceidea\\mvnstudy\\conf\\hbase-site.xml"));
//	        config.addResource(new Path("E:\\workplaceidea\\mvnstudy\\conf\\core-site.xml"));
	    
	    Configuration config = HbaseConfig.getHHConfig();
//	    createSchemaTables(config);
//	    modifySchema(config);
	    
//	    scanSchema(config);
	    
	    
	    // 对数据的操作：
//	    putData(config); // 增加数据

//	    deleteData(config); // 删除数据
//	    getData(config); // 获取数据
	    
	    scanData(config);
	  }
	
	
}
