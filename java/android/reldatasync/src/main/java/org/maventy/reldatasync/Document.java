package org.maventy.reldatasync;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Document extends TreeMap<String, Object> implements Comparable<Document> {
    public static final String ID = "_id";
    /** REV is revision number */
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

        return this.compareTo(doc) == 0;
    }

    @Override
    public Document clone() {
        return new Document(this);
    }

    private int compareVals(Object val1, Object val2) {
        if (val1 == null && val2 == null)
            return 0;
        if (val1 == null && val2 != null)
            return -1;
        if (val1 != null && val2 == null)
            return 1;
        assert val1 != null && val2 != null;

        int ret = val1.getClass().getName().compareTo(val2.getClass().getName());
        if (ret != 0) {
            throw new IllegalArgumentException("classes are not the same");
        }

        // We want only comparable types here
        Class clazz = val1.getClass();

        if (!Comparable.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("classes are not Comparable");
        }

        // It's comparable, so compare
        Comparable<Object> c1 = (Comparable<Object>) val1;
        Comparable<Object> c2 = (Comparable<Object>) val2;
        return (c1.compareTo(c2));
    }

    @Override
    public int compareTo(Document doc) {
        // NOTE: This should return the same result as the python version,
        // otherwise there are corner cases that would synchronize differently.
        // This probably requires a LOT of test coverage to get right.
        // Example: same number of keys but different key names.
        // Example: checking every type of object.

        // self check
        if (this == doc)
            return 0;

        // null check
        if (doc == null)
            return 1;

        if (keySet().size() < doc.keySet().size())
            return -1;
        if (keySet().size() > doc.keySet().size())
            return 1;

        // same number of keys, now compare them
        Comparator<Object> comp = new Comparator<Object>() {
            @Override
            public int compare(Object obj, Object obj2) {
                return compareVals(obj, obj2);
            }
        };
        List<Object> keys = Arrays.asList(keySet().toArray());
        Collections.sort(keys, comp);
        List<Object> docKeys = Arrays.asList(doc.keySet().toArray());
        Collections.sort(docKeys, comp);
        for (int idx = 0; idx < keys.size(); idx++) {
            int keycomp = compareVals(keys.get(idx), docKeys.get(idx));
            if (keycomp != 0)
                return keycomp;
        }

        // All values must be the same (using equals)
        for (String key: keySet()) {
            Object val1 = get(key);
            Object val2 = doc.get(key);
            int ret = compareVals(val1, val2);
            if (ret != 0)
                return ret;
        }

        return 0;
    }
}
