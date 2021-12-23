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

        @Override
        public String toString() {
            return "DatastoreException{" + "cause=" + cause + '}';
        }
    }

    /**
     * Get doc given its id.  If no doc, return null.
     *
     * @param docid  Doc to get
     * @return Doc
     * @throws DatastoreException  If can't get
     */
    Document get(String docid) throws DatastoreException;

    /**
     * Store doc by its id.  If it already exists, replace it.
     *
     * @param doc  Document to put.  If it has no _REV, give it one.
     * @throws DatastoreException  If can't put
     */
    void put(Document doc) throws DatastoreException;

    /**
     * If doc exists, set the deleted flag.  If it doesn't exist, do nothing.
     *
     * @param docid  Doc to delete
     * @throws DatastoreException  If can't delete
     */
    void delete(String docid) throws DatastoreException;
    DocsSinceValue getDocsSince(int theSeq, int num) throws DatastoreException;
}
