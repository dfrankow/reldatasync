package org.maventy.reldatasync.android;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.maventy.reldatasync.Document;
import org.robolectric.RobolectricTestRunner;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
    @Test
    public void test1() throws IOException, SQLException {
        try (AutoDeletingTempFile file = new AutoDeletingTempFile("tmp", ".db");) {
            setConnection(file.getFile());
            assertEquals(1, 1);
        }

        // Table created outside JdbcDatastore
        createTable1();
        JdbcDatastore jds = new JdbcDatastore(conn, "table1");
        UUID id = UUID.randomUUID();
        // remove -s
        String foo = id.toString().replaceAll("-", "");
        assertNull(jds.get(foo));
    }
}
