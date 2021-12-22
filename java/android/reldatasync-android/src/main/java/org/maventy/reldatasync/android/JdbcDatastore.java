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

                List<String> names = getColumnNames(rs);
                for (int idx = 0; idx < names.size(); idx++) {
                    String name = names.get(idx);
                    jo.put(name, rs.getObject(idx+1));
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
