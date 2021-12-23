package org.maventy.reldatasync;

public abstract class BaseDatastore implements Datastore {
    protected int sequenceId;

    protected void incrementSequenceId() {
        sequenceId++;
    }

    protected Document prePut(Document doc) {
        // Copy doc so we don't modify caller's doc
        Document doc1 = doc.clone();
        if (doc.get(Document.REV) == null) {
            // Add sequence number
            incrementSequenceId();
            doc1.put(Document.REV, sequenceId);
        }
        return doc1;
    }

    // TODO(dan): Implement put_if_needed?
    // def put_if_needed(self, doc: Document) -> int
    //            """Put doc under docid if seq is greater
    //
    //        Return number of records actually put (0 or 1).
    //
    //        As a side effect, this updates self.sequence_id if doc[_REV] is larger.
    //        """

    /**
     * Delete a doc in the datastore.
     * Return silently if the doc is not in the datastore.
     *
     * @param docid
     */
    public void delete(String docid) throws DatastoreException {
        Document doc = get(docid);
        if (doc != null && (doc.get(Document.DELETED) == null ||
                !((boolean) doc.get(Document.DELETED)))) {
            // Deletion makes a new rev
            incrementSequenceId();
            doc.put(Document.REV, sequenceId);
            put(doc);
        }
    }

//    def get_peer_sequence_id(self, peer: str) -> int:
//            """Get the seq we have for peer, or zero if we have none."""
//            return self.peer_seq_ids.get(peer, 0)
//
//    def set_peer_sequence_id(self, peer: str, seq: int) -> None:
//            """Set new peer sequence id, if seq > what we have."""
//            if seq > self.get_peer_sequence_id(peer):
//    self.peer_seq_ids[peer] = seq

}
