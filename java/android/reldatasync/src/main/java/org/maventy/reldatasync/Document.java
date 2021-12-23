package org.maventy.reldatasync;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Document extends TreeMap<String, Object> {
    public static final String ID = "_id";
    public static final String REV = "_rev";
    public static final String DELETED = "_deleted";

    public static List<Document> fromDocumentsJson(JSONArray ja) throws ParseException {
        List<Document> docs = new ArrayList<>();
        for (Object obj : ja) {
            JSONObject jo = (JSONObject) obj;
            docs.add(new Document(jo));
        }
        return docs;
    }

    public Document() {}

    public Document(String json) throws ParseException {
        // Parse json from the string and put it in the map
        this((JSONObject) new JSONParser().parse(json));
    }

    /**
     * Constructor for a map of (key, value) pairs.
     *
     * NOTE this constructor works for org.json.simple.JSONObject, which extends a map.
     *
     * @param jo  map of key name to value.
     */
    public Document(Map<String, Object> jo) {
        for (Object obj : jo.entrySet()) {
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
            this.put(entry.getKey(), entry.getValue());
        }
        assert this.containsKey(Document.ID);
    }

    public String toJsonString() {
        JSONObject jo = new JSONObject();
        for (Map.Entry<String, Object> entry : this.entrySet()) {
            jo.put(entry.getKey(), entry.getValue());
        }
        return jo.toJSONString();
    }

    @Override
    public boolean equals(Object obj) {
        // self check
        if (this == obj)
            return true;

        // null check
        if (obj == null)
            return false;

        // type check and cast
        if (getClass() != obj.getClass())
            return false;
        Document doc = (Document) obj;

        // All keys must be the same
        if (!keySet().equals(doc.keySet()))
            return false;

        // All values must be the same (using equals)
        for (String key: keySet()) {
            Object val1 = get(key);
            Object val2 = doc.get(key);
            if (val1 == null && val2 != null)
                return false;
            if (val1 != null && !val1.equals(val2))
                return false;
        }

        return true;
    }

    @Override
    public Document clone() {
        return new Document(this);
    }
}
