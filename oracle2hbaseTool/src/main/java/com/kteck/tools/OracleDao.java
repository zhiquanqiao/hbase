package com.kteck.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleDao {

    public static int getSourceCount(String tableName) throws Exception {

        int result = 0;
        Connection conn = JDBCTools.getConnection(DataType.ORACLE);
        PreparedStatement ps = conn.prepareStatement("select count(*) from " + tableName);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
            result = rs.getInt(1);
        }
        JDBCTools.close(conn, ps, rs);

        return result;
    }

    public static List<ColumnMeta> getTableColumns(String tableName) throws Exception {
        List<ColumnMeta> result = new ArrayList<>();

        Connection conn = JDBCTools.getConnection(DataType.ORACLE);
        String sql = "select *\n" +
                "  from (select a.*, rownum as rnum\n" +
                "          from (select * from " + tableName + ") a\n" +
                "         where rownum <=" + 1 + ")\n" +
                " where rnum >= " + 1;
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData rsMetaData = rs.getMetaData();
        if (rs.next()) {
            int count = rsMetaData.getColumnCount();
            for (int i = 0; i < count-1; i++) {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setName(rsMetaData.getColumnLabel(i + 1));
                columnMeta.setType(rsMetaData.getColumnTypeName(i + 1));
                result.add(columnMeta);
            }
        }
        JDBCTools.close(conn, ps, rs);


        return result;
    }


    public static List<List<ColumnValue>> getTableValue(String tableName, long start, long end, List<ColumnMeta> tableColumns) throws Exception {

        List<List<ColumnValue>> result = new ArrayList<>();
        String sql = "select *\n" +
                "  from (select a.*, rownum as rnum\n" +
                "          from (select * from " + tableName + ") a\n" +
                "         where rownum <" + end + ")\n" +
                " where rnum >= " + start;

        Connection conn = JDBCTools.getConnection(DataType.ORACLE);
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            ColumnValue columnValue = null;
            List<ColumnValue> table = new ArrayList<>();
            for (ColumnMeta columnMeta : tableColumns) {
                columnValue = new ColumnValue(columnMeta.getName(), rs.getObject(columnMeta.getName()));
                table.add(columnValue);
            }
            result.add(table);
        }

        JDBCTools.close(conn, ps, rs);
        return result;

    }

}
