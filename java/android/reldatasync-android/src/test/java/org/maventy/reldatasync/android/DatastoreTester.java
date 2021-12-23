package org.maventy.reldatasync.android;

import org.maventy.reldatasync.Datastore;
import org.maventy.reldatasync.Document;

import java.util.HashMap;

/**
 * Used to unit test a Datastore class by getting them from a DatastoreFactory.
 * See also python code test_sync.py.
 */
public class DatastoreTester {
    interface DatastoreFactory {
        Datastore getDatastore(String name);
    }

    protected DatastoreFactory datastoreFactory;

    public DatastoreTester(DatastoreFactory datastoreFactory) {
        this.datastoreFactory = datastoreFactory;
    }

    public void testNonoverlapping() throws Datastore.DatastoreException {
        Datastore server = datastoreFactory.getDatastore("ds1");
        Datastore client = datastoreFactory.getDatastore("ds2");

        // server makes object A v1
        server.putIfNeeded(new Document(new HashMap<String, Object>() {{
            put(Document.ID, "A");
            put("value", "val1");
        }}));

        // client makes object B v1
        client.putIfNeeded(new Document(new HashMap<String, Object>() {{
            put(Document.ID, "B");
            put("value", "val2");
        }}));
//        # sync leaves both server and client with A val1, B val2
//        self.client.sync_both_directions(self.server)
//
//        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
//        self.client.get('A'))
//        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
//        self.client.get('B'))
//
//        self.assertEqual(Document({_ID: 'A', 'value': 'val1', _REV: 1}),
//        self.server.get('A'))
//        self.assertEqual(Document({_ID: 'B', 'value': 'val2', _REV: 1}),
//        self.server.get('B'))
//
//        # counter is at the highest existing doc version
//        server_seq, server_docs = self.server.get_docs_since(0, 1000)
//        self.assertEqual(self.server.sequence_id, server_seq)
//        self.assertEqual(self.server.sequence_id,
//                max(doc[_REV] for doc in server_docs))
//
//        client_seq, client_docs = self.client.get_docs_since(0, 1000)
//        self.assertEqual(self.client.sequence_id, client_seq)
//        self.assertEqual(self.client.sequence_id,
//                max(doc[_REV] for doc in client_docs))
    }
}
