package com.tony.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class TableManager {
	  // 表名
	  private static final String TABLE_NAME = "bd_table";
	  // cf名
	  private static final String CF_DEFAULT = "portid";

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
	    
	    // 列出hbase现有的所有表
	    HTableDescriptor[] tableDescriptor = admin.listTables();
	    for(int i=0;i<tableDescriptor.length;i++) {
	    	System.out.println(tableDescriptor[i].getNameAsString());
	    }
	    
	  }
	 
	  
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
	  
	public static void deleteData(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);

		Delete delete = new Delete(Bytes.toBytes("debugo")); 
		table.delete(delete);  
		table.close();
	}
	
	/**
	 * 查询遍历输出所有数据
	 * @param config
	 * @throws IOException
	 */
	public static void scanAllRows(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);
		
		// 遍历所有
		Scan scan = new Scan(); 
		ResultScanner resultScaner = table.getScanner(scan);
		for (Result result : resultScaner) {
			for (Cell cell : result.listCells()) {
				System.out.println("row:      " +Bytes.toString(CellUtil.cloneRow(cell)));
				System.out.println("family:   " +Bytes.toString(CellUtil.cloneFamily(cell)));
				System.out.println("qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println("value:    " +Bytes.toString(CellUtil.cloneValue(cell)));
				System.out.println("timestamp:" +cell.getTimestamp());
				System.out.println("-------------------------------------------");
			}
	    }
		
		// 只遍历Row-key为"debugo"的内容
		Get get = new Get(Bytes.toBytes("debugo"));
		Result result = table.get(get);
		for (Cell cell : result.listCells()) {
			System.out.println("row:      " +Bytes.toString(CellUtil.cloneRow(cell)));
			System.out.println("family:   " +Bytes.toString(CellUtil.cloneFamily(cell)));
			System.out.println("qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("value:    " +Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("timestamp:" +cell.getTimestamp());
			System.out.println("-------------------------------------------");
		}
		
		table.close();
	}
	
	/**
	 * 查询单行数据
	 * @param config
	 * @throws IOException
	 */
	public static void getRowData(Configuration config) throws IOException {
		Connection connection = ConnectionFactory.createConnection(config);
		TableName tablename = TableName.valueOf("member");
		Table table = connection.getTable(tablename);
		// 根据row-key/family/qualifier三者查询值
		Get get = new Get(Bytes.toBytes("debugo"));
		Result result = table.get(get);
		String str = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
		System.out.println(str);
		table.close();
	}

	  public static void main(String... args) throws IOException {
//	    Configuration config = HBaseConfiguration.create();
//	    //Add any necessary configuration files (hbase-site.xml, core-site.xml)
//	    config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//	    config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		  
//	        config.addResource(new Path("E:\\workplaceidea\\mvnstudy\\conf\\hbase-site.xml"));
//	        config.addResource(new Path("E:\\workplaceidea\\mvnstudy\\conf\\core-site.xml"));
	    
	    Configuration config = HbaseConfig.getHHConfig();
	    // 创建表
	    createSchemaTables(config);
//	    modifySchema(config);
	    
//	    scanSchema(config);
	    
	    
	    // 对数据的操作：
//	    putData(config); // 增加数据

//	    deleteData(config); // 删除数据
	    // 批量删除
	    
//	    getRowData(config); // 获取数据
	    
//	    scanAllRows(config);
	  }
	
	
}
