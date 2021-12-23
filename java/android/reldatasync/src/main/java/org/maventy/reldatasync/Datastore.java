package org.maventy.reldatasync;

import java.util.List;

public interface Datastore {
    public class DocsSinceValue {
        public final int currentSequenceId;
        public final List<Document> documents;
        public DocsSinceValue(int currentSeq, List<Document> docs) {
            this.currentSequenceId = currentSeq;
            this.documents = docs;
        }
    };

    public class DatastoreException extends Exception {
        public final Throwable cause;
        DatastoreException(Throwable cause) {
            this.cause = cause;
        }
    }

    public Document get(String docid) throws DatastoreException;
    public void put(Document doc) throws DatastoreException;
    public void delete(String docid);
    public DocsSinceValue getDocsSince(int theSeq, int num) throws DatastoreException;
}
