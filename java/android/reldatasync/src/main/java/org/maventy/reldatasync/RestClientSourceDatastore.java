package org.maventy.reldatasync;

import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Communicate to a REST server.
public class RestClientSourceDatastore {
    private final String table;
    private final String baseurl;

    public RestClientSourceDatastore(String baseurl, String table) {
        this.table = table;
        this.baseurl = baseurl;
    }

    private String serverUrl(String rest) {
        return this.baseurl + this.table + rest;
    }

    public Document get(String docid) throws IOException, ParseException {
        Document ret = null;
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(serverUrl("/doc/" + docid))
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 200) {
                ret = new Document(response.body().string());
            }
        }

        return ret;
    }

    public void put(@NotNull final Document doc) throws IOException {
        OkHttpClient client = new OkHttpClient();

        RequestBody formBody = new FormBody.Builder()
                .add("json", doc.toJsonString())
                .build();

        Request request = new Request.Builder()
                .url(serverUrl("/doc"))
                .post(formBody)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException(
                        response.code() + ": " + response.message());
            }
        }
    }

    public static class DocsSinceValue {
        public int currentSequenceId;
        public List<Document> documents;
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) throws IOException, ParseException {
        DocsSinceValue ret = null;
        OkHttpClient client = new OkHttpClient();
        RequestBody formBody = new FormBody.Builder()
                .add("start_sequence_id", "" + theSeq)
                .add("chunk_size", "" + num)
                .build();
        Request request = new Request.Builder()
                .url(serverUrl("/docs"))
                .post(formBody)
                .build();

        try (Response response = client.newCall(request).execute()) {
            if (response.code() == 200) {
                String respBody = response.body().string();

                // ret = js['current_sequence_id'], js['documents']
                // Parse json from the string and put it in the map
                JSONObject jo = (JSONObject) new JSONParser().parse(respBody);
                int currentSequenceId = (Integer) jo.get("current_sequence_id");
                List<Document> docs = new ArrayList<>();
                JSONArray ja = (JSONArray) jo.get("documents");
                for (Object obj : ja) {
                    docs.add(new Document((JSONObject) obj));
                }

                ret = new DocsSinceValue();
                ret.currentSequenceId = currentSequenceId;
                ret.documents = docs;
            }
        }

        return ret;
    }
}
