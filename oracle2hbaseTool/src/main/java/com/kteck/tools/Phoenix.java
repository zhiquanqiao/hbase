package com.kteck.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Phoenix {


//    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static void main(String[] args) throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\dev tools\\hadoop-common-2.2.0-bin-master");


        Class.forName(YamlReader.getInstance().getStringValueByKey("oracle", "driverClassName"));
        String url = YamlReader.getInstance().getStringValueByKey("oracle", "url");
        String userName = YamlReader.getInstance().getStringValueByKey("oracle", "username");
        String password = YamlReader.getInstance().getStringValueByKey("oracle", "password");
        Connection con = DriverManager.getConnection(url, userName, password);

        Statement stmt = con.createStatement();
        String sql = "select *\n" +
                "  from (select a.*, rownum as rnum\n" +
                "          from (select * from  C2019_000000_SD601_11) a\n" +
                "         where rownum <" + 1000000 + ")\n" +
                " where rnum >= " + 0;
        ResultSet rs = stmt.executeQuery(sql);
        while(rs.next()){
        }
        stmt.close();
        con.close();


    }
}