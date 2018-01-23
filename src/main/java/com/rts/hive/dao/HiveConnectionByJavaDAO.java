package com.rts.hive.dao;

import java.sql.*;

public class HiveConnectionByJavaDAO {

    /**
     * 网上很多相关教程的driverName值都是： "org.apache.hadoop.hive.jdbc.HiveDriver"，是错误的。
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        // 因为服务器启动的是HiveServer2，所以此处的连接不是"jdbc:hive://"，应该为"jdbc:hive2://"
        // "hadoop"参数对应部署hive所在服务器的用户名，需要在hadoop的core-site.xml文件中配置；hive是表名，需要在hive的hive-site.xml文件中配置
        // 且服务器端需要外的启动“bin/hive --service hiveserver2 &”服务
        Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hadoop", "hive");
        Statement stmt = con.createStatement();

        String tableName = "hive_bdtable";
        // select * query
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }
        // select * query
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getString(1)) + " " + res.getDouble(2) + " " + res.getDouble(3));
        }
        System.out.println("Database userdb created successfully.");
        con.close();
    }
}
