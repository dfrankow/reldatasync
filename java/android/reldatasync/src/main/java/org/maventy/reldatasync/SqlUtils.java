package org.maventy.reldatasync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlUtils {
    static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, tableName, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    /**
     * Return list of column names for table from conn.
     *
     * @param conn  Connection
     * @param tableName  Table name (or pattern)
     * @return list of column names
     * @throws SQLException  If things go wrong
     */
    static List<String> getTableColumnNames(Connection conn, String tableName) throws SQLException {
        List<String> names = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, null, tableName, null);
        while (rs.next()) {
            names.add(rs.getString("COLUMN_NAME"));
        }
        return names;
    }

    /**
     * Get column names from a ResultSet.
     *
     * @param rs  ResultSet
     * @return  List of names
     * @throws SQLException  If things go wrong
     */
    static List<String> getResultSetColumnNames(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for(int idx = 1; idx <=count; idx++) {
            String name = meta.getColumnName(idx);
            names.add(name);
        }
        return names;
    }
}
