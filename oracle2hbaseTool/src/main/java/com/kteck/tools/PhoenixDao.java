package com.kteck.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

public class PhoenixDao {


    public static boolean insert(List<List<ColumnValue>> tablesValue, String tableName) throws Exception {
        boolean flag = false;


        Connection conn = JDBCTools.getConnection(DataType.PHOENIX);

        String sql = getSql(tableName, tablesValue.get(0));
        PreparedStatement ps = conn.prepareStatement(sql);

        conn.setAutoCommit(false);
        for (List<ColumnValue> tableValue : tablesValue) {
            for (int i = 1; i <= tableValue.size(); i++) {
                ps.setObject(i, tableValue.get(i - 1).getValue());
            }
            ps.addBatch();
        }
        int[] result = ps.executeBatch();
        System.out.println(result.length);
        conn.commit();
        JDBCTools.close(conn, ps, null);

        flag = true;
        return flag;
    }


    private static String getSql(String tableName, List<ColumnValue> tableValue) {

        String result = null;
        if (tableValue != null && tableValue.size() > 0) {
            StringBuffer sqlBuffer = new StringBuffer();
            StringBuffer valueBuffer = new StringBuffer();
            sqlBuffer.append("upsert into C2019_000000_SD601_11_NEW (");
            valueBuffer.append(" values (");
            for (int i = 0; i < tableValue.size(); i++) {
                if (i > 0) {
                    valueBuffer.append(",");
                    sqlBuffer.append(",");
                }
                valueBuffer.append("?");
                sqlBuffer.append(tableValue.get(i).getName());

            }
            sqlBuffer.append(") ");
            valueBuffer.append(")");
            sqlBuffer.append(valueBuffer);
            result = sqlBuffer.toString();
        }

        return result;


    }


}
