package org.maventy.reldatasync.android;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.maventy.reldatasync.Document;

// Communicate to a REST server.
public class JdbcDatastore {
    private final Connection conn;
    private final String table;

    public JdbcDatastore(Connection conn, String table) {
        this.conn = conn;
        this.table = table;
    }

    private static class NamesAndTypes {
        public final List<String> names;
        public final List<Integer> types;
        public NamesAndTypes(List<String> names, List<Integer> types) {
            this.names = names;
            this.types = types;
        }
    };

    private static NamesAndTypes getColumnNamesAndTypes(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<>();
        List<Integer> types = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for(int idx = 1; idx <=count; idx++) {
            String name = meta.getColumnName(idx);
            names.add(name);
            types.add(meta.getColumnType(idx));
        }
        return new NamesAndTypes(names, types);
    }

    private static List<String> getColumnNames(ResultSet rs) throws SQLException {
        List<String> names = new ArrayList<>();
        ResultSetMetaData meta = rs.getMetaData();
        int count = meta.getColumnCount();
        for(int idx = 1; idx <=count; idx++) {
            String name = meta.getColumnName(idx);
            names.add(name);
        }
        return names;
    }

    public Document get(String docid) throws SQLException {
        Document doc = null;
        String sql = "SELECT * FROM " + table + " WHERE " + Document.ID + " = ?";
        try (PreparedStatement getSt = conn.prepareStatement(sql)) {
            getSt.setString(1, docid);
            ResultSet rs = getSt.executeQuery();
            if (rs.next()) {
                JSONObject jo = new JSONObject();

                NamesAndTypes nat = getColumnNamesAndTypes(rs);
                for (int idx = 0; idx < nat.names.size(); idx++) {
                    int type = nat.types.get(idx);
                    // Check if type is supported
                    if (!(type == Types.INTEGER || type == Types.CHAR)) {
                        throw new SQLException("Unsupported type " + type);
                    }

                    String name = nat.names.get(idx);
                    jo.put(name, rs.getObject(idx));
                }
                doc = new Document(jo);
            }
        }
        return doc;
    }

    public void put(final Document doc) throws IOException {
    }

    public static class DocsSinceValue {
        public int currentSequenceId;
        public List<Document> documents;
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) throws IOException, ParseException {
        return null;
    }
}
