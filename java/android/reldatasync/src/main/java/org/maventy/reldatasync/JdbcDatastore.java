package org.maventy.reldatasync;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

// Communicate to a REST server.
public class JdbcDatastore implements Datastore {
    private final Connection conn;
    private final String table;

    public JdbcDatastore(Connection conn, String table) {
        this.conn = conn;
        this.table = table;

        // TODO(dan): Init sequence_id if not present
        //         # Check that the right tables exist
        //         # Get the column names for self.tablename
        // TODO(dan): Check that the table has ID, REV, and DELETED
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

    public Document get(String docid) throws DatastoreException {
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
        } catch (SQLException throwables) {
            throw new DatastoreException(throwables);
        }
        return doc;
    }

    private void pre_put(Document doc) {
//        # copy doc so we don't modify caller's doc
//        doc = doc.copy()
//
//        if doc.get(_REV, None) is None:
//        self._increment_sequence_id()
//        doc[_REV] = self._sequence_id
//
//        return doc
    }
    public void put(final Document doc) throws DatastoreException {
//        """Put doc under docid.
//
//        If no seq, give it one.
//        """
//        doc = self._pre_put(doc)
//
//        # "ON CONFLICT" requires postgres 9.5+
//                set_statement = ', '.join("%s=EXCLUDED.%s " % (col, col)
//        for col in self.columnnames)
//        upsert_statement = (
//                "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (_id) DO UPDATE"
//        " SET %s" % (
//                self.tablename,
//                ','.join(self.columnnames),
//                ','.join([r'%s' for _ in self.columnnames]),
//        set_statement))
//
//        self.cursor.execute(
//                upsert_statement,
//                tuple([doc.get(key, None) for key in self.columnnames]))
    }

    @Override
    public void delete(String docid) {
        // TODO(dan):
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) throws DatastoreException {
        return null;
    }
}
