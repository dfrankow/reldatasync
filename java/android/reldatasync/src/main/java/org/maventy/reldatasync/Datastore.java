package org.maventy.reldatasync;

import java.util.List;

public interface Datastore {
    class DocsSinceValue {
        public final int currentSequenceId;
        public final List<Document> documents;
        public DocsSinceValue(int currentSeq, List<Document> docs) {
            this.currentSequenceId = currentSeq;
            this.documents = docs;
        }
    };

    class DatastoreException extends Exception {
        public final Throwable cause;
        DatastoreException(Throwable cause) {
            this.cause = cause;
        }
    }

    Document get(String docid) throws DatastoreException;
    void put(Document doc) throws DatastoreException;
    void delete(String docid);
    DocsSinceValue getDocsSince(int theSeq, int num) throws DatastoreException;
}
