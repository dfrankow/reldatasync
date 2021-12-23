package org.maventy.reldatasync.android;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.maventy.reldatasync.Document;
import org.maventy.reldatasync.JdbcDatastore;
import org.robolectric.RobolectricTestRunner;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(RobolectricTestRunner.class)
public class TestJdbcDataStore {
    private Connection conn = null;

    private void setConnection(File file) throws SQLException {
        try {
            DriverManager.registerDriver((Driver) Class.forName("org.sqldroid.SQLDroidDriver").newInstance());
        } catch (Exception e) {
            throw new RuntimeException("Failed to register SQLDroidDriver");
        }
        String jdbcUrl = "jdbc:sqldroid:" + file.getAbsolutePath();
        this.conn = DriverManager.getConnection(jdbcUrl);
    }

    private void createTable1() throws SQLException {
        String sql = "CREATE TABLE table1 " +
                "("+ Document.ID + " VARCHAR(32) not NULL, " +
                " first VARCHAR(255), " +
                " last VARCHAR(255), " +
                " age INTEGER, " +
                " PRIMARY KEY ( " + Document.ID + " ))";

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    private static String randomUUIDString() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    @Test
    public void test1() throws IOException, SQLException {
        try (AutoDeletingTempFile file = new AutoDeletingTempFile("tmp", ".db");) {
            setConnection(file.getFile());
        }

        // Table created outside JdbcDatastore
        createTable1();
        JdbcDatastore jds = new JdbcDatastore(conn, "table1");
        // remove -s
        String id = randomUUIDString();
        assertNull(jds.get(id));

        // Add row to table1
        String sql = "insert into table1 (" + Document.ID + ", first, last, age)" +
                " VALUES(?, ?, ?, ?)";
        String first = "a first";
        String last = "a last";
        int age = 40;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, first);
            ps.setString(3, last);
            ps.setInt(4, age);
            ps.execute();
        }
        Document doc = jds.get(id);
        assertEquals(first, doc.get("first"));
        assertEquals(last, doc.get("last"));
        assertEquals(id, doc.get(Document.ID));
        assertEquals(age, doc.get("age"));

        // Put a doc into the DB
//        final String id2 = randomUUIDString();
//        final String first2 = "first 2";
//        final String last2 = "last 2";
//        final int age2 = 50;
//        doc = new Document(
//                new HashMap<String, Object>() {{
//                    put(Document.ID, id2);
//                    put("first", first2);
//                    put("last", last2);
//                    put("age", age2);
//                }}
//        );
//        jds.put(doc);
//        Document doc2 = jds.get(id2);
//        assertEquals(doc, doc2);
    }
}
