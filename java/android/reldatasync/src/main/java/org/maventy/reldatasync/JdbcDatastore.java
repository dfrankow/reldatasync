package org.maventy.reldatasync;

import org.json.simple.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Use datastore through a JDBC connection.
 * NOTE: currently only works with sqlite (because of ON CONFLICT SQL).
 */
public class JdbcDatastore extends BaseDatastore {
    private final Connection conn;
    private final String tableName;
    private final List<String> columnNames;
    private final String upsertStatement;

    public JdbcDatastore(Connection conn, String tableName) throws DatastoreException {
        this.conn = conn;
        this.tableName = tableName;

        try {
            this.columnNames = SqlUtils.getTableColumnNames(conn, tableName);
        } catch (SQLException throwable) {
            throw new DatastoreException(throwable);
        }
        // TODO(dan): Init sequence_id if not present
        //         # Check that the right tables exist
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
        // "ON CONFLICT" requires postgres 9.5+ or sqlite
        List<String> elems = new ArrayList<>();
        for (String col : columnNames) {
            elems.add(String.format("%s=EXCLUDED.%s", col, col));
        }
        String setStatement = StringUtils.join(", ", elems);

        List<String> questions = new ArrayList<>();
        for (int idx = 0; idx < columnNames.size(); idx++) {
            questions.add("?");
        }

        // This version works in sqlite
        // See https://database.guide/how-on-conflict-works-in-sqlite/
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
        return null;
    }
}
