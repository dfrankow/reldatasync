package org.maventy.reldatasync;

import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Use datastore through a JDBC connection.
 * NOTE: currently only works with sqlite (because of ON CONFLICT SQL syntax).
 */
public class JdbcDatastore extends BaseDatastore {
    public static final String SYNC_TABLE_NAME = "reldatasync_revisions";

    private final Connection conn;
    private final String tableName;
    private final List<String> columnNames;
    private final String upsertStatement;

    public JdbcDatastore(Connection conn, String id, String tableName) throws DatastoreException {
        super(id);

        this.conn = conn;
        this.tableName = tableName;

        try {
            this.columnNames = SqlUtils.getTableColumnNames(conn, tableName);
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }

        // Init sequence_id if not present
        // "OR IGNORE" is a sqlite extension (from "ON CONFLICT")
        // See https://database.guide/how-on-conflict-works-in-sqlite/
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT OR IGNORE INTO " + SYNC_TABLE_NAME
                + " (datastore_id, sequence_id)"
                + " VALUES (?, 0)")) {
            ps.setString(1, id);
            ps.executeUpdate();
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }

        // Initialize sequence id
        super.setSequenceId(getSequenceId());

        // TODO: Check that sequence_id in revisions table is >= max(REV)
        //   in the data

        // Check that the table has ID, REV, and DELETED
        for (String name: new ArrayList<String>() {{
            add(Document.ID);
            add(Document.REV);
            add(Document.DELETED); }}) {
            if (!columnNames.contains(name)) {
                throw new DatastoreException(
                        "Table " + tableName + " does not contain column '" + name + "'");
            }
        }

        this.upsertStatement = putStatement();
    }

    public static void createSequenceIdsTable(Connection conn) throws SQLException {
        if (!SqlUtils.tableExists(conn, SYNC_TABLE_NAME)) {
            String sql = "CREATE TABLE " + SYNC_TABLE_NAME +
                    " (datastore_id VARCHAR(100) not NULL, " +
                    "  sequence_id INTEGER not NUlL, " +
                    "  PRIMARY KEY ( datastore_id ))";

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sql);
            }
        }
    }

    public Document get(String docid) throws DatastoreException {
        Document doc = null;
        String sql = "SELECT * FROM " + tableName + " WHERE " + Document.ID + " = ?";
        try (PreparedStatement getSt = conn.prepareStatement(sql)) {
            getSt.setString(1, docid);
            ResultSet rs = getSt.executeQuery();
            if (rs.next()) {
                JSONObject jo = new JSONObject();

                List<String> names = columnNames;
                for (int idx = 0; idx < names.size(); idx++) {
                    String name = names.get(idx);
                    jo.put(name, rs.getObject(idx+1));
                }
                doc = new Document(jo);

                // Any doc in the datastore should have REV
                if (!doc.containsKey(Document.REV)) {
                    throw new DatastoreException(
                            "Document " + doc.get(Document.ID) + " does not have " + Document.REV);
                }
            }
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }

        return doc;
    }

    private String putStatement() {
        List<String> questions = new ArrayList<>();
        for (int idx = 0; idx < columnNames.size(); idx++) {
            questions.add("?");
        }

        // This version works in sqlite
        // See https://database.guide/how-on-conflict-works-in-sqlite/
        // "OR REPLACE" is a sqlite extension (from "ON CONFLICT")
        // https://sqlite.org/lang_conflict.html
        return String.format(
                "INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
                tableName,
                StringUtils.join(",", columnNames),
                StringUtils.join(",", questions)
        );
    }

    protected void put(final Document doc) throws DatastoreException {
        Document doc1 = prePut(doc);

//        System.out.println(upsertStatement);
        // TODO: cache prepared statement at the class level?
        try (PreparedStatement ps = conn.prepareStatement(upsertStatement)) {
            for (int idx = 0; idx < columnNames.size(); idx++) {
                String col = columnNames.get(idx);
                Object val = doc1.get(col);
                ps.setObject(idx+1, val);
//                System.out.println("setObject(" + (idx+1) + ", " + val + ")");
            }
            ps.executeUpdate();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
            throw new DatastoreException(throwable);
        }
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) throws DatastoreException {
        throw new DatastoreException("TODO: implement");
    }

    @Override
    protected void incrementSequenceId() throws DatastoreException {
        // Would like to use RETURNING, but it's not supported until sqlite 3.35.0 (2021-03-12)
        // See https://www.sqlite.org/draft/lang_returning.html
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE " + SYNC_TABLE_NAME
                + " set sequence_id = sequence_id+1"
                + " WHERE datastore_id=?")) {
            ps.setString(1, id);
            ps.executeUpdate();
            super.setSequenceId(getSequenceId());
            System.out.println("JDBC increment to " + sequenceId);
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }
    }

    public int getSequenceId() throws DatastoreException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT sequence_id FROM " + SYNC_TABLE_NAME + " WHERE datastore_id = ?")) {
             ps.setString(1, id);
             try (ResultSet rs = ps.executeQuery()) {
                 rs.next();
                 return rs.getInt("sequence_id");
             }
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }
    }

    @Override
    protected void setSequenceId(int seq) throws DatastoreException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT sequence_id FROM " + SYNC_TABLE_NAME)) {
            rs.next();
            super.setSequenceId(rs.getInt("sequence_id"));
            System.out.println("JDBC set to " + sequenceId);
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }
    }
}
