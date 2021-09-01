package org.maventy.reldatasync;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

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

    public Document get(String docid) {
        Document ret = null;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl("/doc/" + docid)))
                .build();
        try {
            HttpResponse<String> resp =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                ret = new Document(resp.body());
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return ret;
    }

    public void put(@NotNull final Document doc) throws IOException {
        var client = HttpClient.newHttpClient();

        var paramValues = new HashMap<>() {{
            put("json", doc.toJsonString());
        }};

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl("/doc")))
                .POST(NetworkUtil.ofFormData(paramValues))
                .build();
        try {
            HttpResponse<String> resp =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            throw(new IOException(e));
        }
    }

    public static class DocsSinceValue {
        public int currentSequenceId;
        public List<Document> documents;
    }

    public DocsSinceValue getDocsSince(final int theSeq, final int num) {
        DocsSinceValue ret = null;
        HttpClient client = HttpClient.newHttpClient();
        var paramValues = new HashMap<>() {{
            put("start_sequence_id", theSeq);
            put("chunk_size", num);
        }};
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl("/docs")))
                .POST(NetworkUtil.ofFormData(paramValues))
                .build();

        try {
            HttpResponse<String> resp =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

            int status = resp.statusCode();
            if (status == 200) {
                String respBody = resp.body();

                // ret = js['current_sequence_id'], js['documents']
                // Parse json from the string and put it in the map
                JSONObject jo = (JSONObject) new JSONParser().parse(resp);
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
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

        return ret;
    }
}
