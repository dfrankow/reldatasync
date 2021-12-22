package org.maventy.reldatasync.android;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.robolectric.RobolectricTestRunner;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

@RunWith(RobolectricTestRunner.class)
public class TestJdbcDataStore {
    private Connection connection = null;

    private void setConnection(File file) throws SQLException {
        try {
            DriverManager.registerDriver((Driver) Class.forName("org.sqldroid.SQLDroidDriver").newInstance());
        } catch (Exception e) {
            throw new RuntimeException("Failed to register SQLDroidDriver");
        }
        String jdbcUrl = "jdbc:sqldroid:" + file.getAbsolutePath();
        this.connection = DriverManager.getConnection(jdbcUrl);
    }

    @Test
    public void test1() throws IOException, SQLException {
        try (AutoDeletingTempFile file = new AutoDeletingTempFile("tmp", ".db");) {
            setConnection(file.getFile());
            assertEquals(1, 1);
        }
    }
}
