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

    public Document(JSONObject jo) {
        for (Object obj : jo.entrySet()) {
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
            this.put(entry.getKey(), entry.getValue());
        }
        assert this.containsKey(Document.ID);
    }

    public String toJsonString() {
        JSONObject jo = new JSONObject();
        for (Map.Entry<String, Object> obj : this.entrySet()) {
            Map.Entry<String, Object> entry = (Map.Entry<String, Object>) obj;
            jo.put(entry.getKey(), entry.getValue());
        }
        return jo.toJSONString();
    }
}
