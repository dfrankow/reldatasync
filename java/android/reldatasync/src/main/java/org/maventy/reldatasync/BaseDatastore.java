package org.maventy.reldatasync;

public abstract class BaseDatastore implements Datastore {
    protected int sequenceId;

    protected void incrementSequenceId() {
        sequenceId++;
    }

    protected void setSequenceId(int seq) {
        sequenceId = seq;
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

    public boolean putIfNeeded(Document doc) throws DatastoreException {
        boolean put = false;
        Object docid = doc.get(Document.ID);
        // # If there is no revision, treat it like rev 0
        Integer seq = (Integer) doc.get(Document.REV);
        if (seq == null) {
            seq = 0;
        }
        Document myDoc = get((String)docid);
        assert myDoc == null || myDoc.get(Document.REV) != null :
            "myDoc should have REV";
        Integer mySeq = null;
        if (myDoc != null)
            mySeq = (Integer) myDoc.get(Document.REV);

        // If my doc is older, or equal time but smaller
        if ((mySeq == null) || (mySeq < seq) || (
                mySeq.equals(seq) && myDoc.compareTo(doc) < 0)) {
            assert mySeq == null || mySeq < seq;
            put(doc);
            put = true;
            // if this doc has a higher rev than our clock, move our clock up
            // NOTE: this may be optional if we handle it at the sync level
            if (seq > sequenceId) {
                setSequenceId(seq);
            }
        }
        return put;
    }

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

    protected abstract void put(final Document doc) throws DatastoreException;

//    def get_peer_sequence_id(self, peer: str) -> int:
//            """Get the seq we have for peer, or zero if we have none."""
//            return self.peer_seq_ids.get(peer, 0)
//
//    def set_peer_sequence_id(self, peer: str, seq: int) -> None:
//            """Set new peer sequence id, if seq > what we have."""
//            if seq > self.get_peer_sequence_id(peer):
//    self.peer_seq_ids[peer] = seq

}
