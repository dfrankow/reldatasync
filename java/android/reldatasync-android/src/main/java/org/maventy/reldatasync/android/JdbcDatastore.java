package org.maventy.reldatasync.android;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.maventy.reldatasync.Document;

// Communicate to a REST server.
public class JdbcDatastore {
    private final String table;

    public JdbcDatastore(String table) {
        this.table = table;
    }

    public Document get(String docid) {
        return null;
    }

    public void put(final Document doc) throws IOException {
    }

    public static class DocsSinceValue {
        public int currentSequenceId;
        public List<Document> documents;
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) throws IOException, ParseException {
        return null;
    }
}
