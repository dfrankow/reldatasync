package org.maventy.reldatasync;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Document extends TreeMap<String, String> {
    private static final String ID = "_id";

    public Document(String json) throws ParseException {
        // Parse json from the string and put it in the map
        this((JSONObject) new JSONParser().parse(json));
    }

    public Document(JSONObject jo) {
        for (Object obj : jo.entrySet()) {
            Map.Entry entry = (Map.Entry) obj;
            this.put((String) entry.getKey(), (String) entry.getValue());
        }
        assert this.containsKey(Document.ID);
    }

    public String toJsonString() {
        JSONObject jo = new JSONObject();
        for (Map.Entry<String, String> stringStringEntry : this.entrySet()) {
            Map.Entry entry = (Map.Entry) stringStringEntry;
            jo.put((String) entry.getKey(), (String) entry.getValue());
        }
        return jo.toJSONString();
    }
}
