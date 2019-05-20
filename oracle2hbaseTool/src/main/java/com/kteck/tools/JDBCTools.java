package com.kteck.tools;

import java.sql.*;
import java.util.Properties;

public class JDBCTools {

    public static Connection getConnection(DataType type) throws Exception {

        Connection result = null;
        if (type == DataType.ORACLE) {
            Class.forName(YamlReader.getInstance().getStringValueByKey("oracle", "driverClassName"));
            String url = YamlReader.getInstance().getStringValueByKey("oracle", "url");
            String userName = YamlReader.getInstance().getStringValueByKey("oracle", "username");
            String password = YamlReader.getInstance().getStringValueByKey("oracle", "password");
            result = DriverManager.getConnection(url, userName, password);

        } else if (type == DataType.PHOENIX) {
            Properties props = new Properties();
            props.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");
            props.setProperty("phoenix.mutate.batchSize", "15000000");
            props.setProperty("phoenix.mutate.maxSize", "2000000");
            props.setProperty("phoenix.mutate.maxSizeBytes", "1048576000");
            Class.forName(YamlReader.getInstance().getStringValueByKey("phoenix", "driverClassName"));
            String url = YamlReader.getInstance().getStringValueByKey("phoenix", "url");

            result = DriverManager.getConnection(url, props);
        }
        return result;
    }

    /**
     * release resource
     *
     * @param conn
     * @param ps
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        try {
            if (rs != null) rs.close();
            rs = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (ps != null) ps.close();
            ps = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            if (conn != null) conn.close();
            conn = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
