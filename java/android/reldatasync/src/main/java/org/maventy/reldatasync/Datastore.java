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
        private final Throwable cause;
        private final String msg;

        DatastoreException(Throwable cause) {
            this.cause = cause;
            this.msg = null;
        }

        DatastoreException(String msg) {
            cause = null;
            this.msg = msg;
        }

        @Override
        public String toString() {
            StringBuilder ret = new StringBuilder();
            ret.append("DatastoreException{");
            if (cause != null) {
                assert msg == null;
                ret.append("cause=" + cause);
            }
            if (msg != null) {
                assert cause == null;
                ret.append("message=" + msg);
            }
            ret.append('}');
            return ret.toString();
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
     * Store doc by its id, only if it does not exist or is greater than the existing doc of that id.
     *
     * "Greater" is defined by Document.compareTo.
     *
     * As a side effect, this updates this.sequenceId if doc[REV] is larger.
     *
     * @param doc  Document to put.  If it has no _REV, give it one.
     * @throws DatastoreException  If can't put
     * @return true if document was put, false if it did not need to be.
     */
    boolean putIfNeeded(Document doc) throws DatastoreException;

    /**
     * If doc exists, set the deleted flag.  If it doesn't exist, do nothing.
     *
     * @param docid  Doc to delete
     * @throws DatastoreException  If can't delete
     */
    void delete(String docid) throws DatastoreException;
    DocsSinceValue getDocsSince(int theSeq, int num) throws DatastoreException;
}
