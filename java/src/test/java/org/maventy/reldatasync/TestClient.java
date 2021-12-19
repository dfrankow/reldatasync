package org.maventy.reldatasync;

import okhttp3.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestClient {
    private static final String APPLICATION_JSON = "application/json";

    private static Response post(String url, String bodyJsonData) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request.Builder builder = new Request.Builder()
                .url(url);

        RequestBody body = new FormBody.Builder().build();  // empty
        if (bodyJsonData != null) {
            body = RequestBody.create(
                    bodyJsonData, MediaType.parse(APPLICATION_JSON));
        }
        Request request = builder.post(body).build();

        return client.newCall(request).execute();
    }

    private static Response get(String url) throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .build();

        return client.newCall(request).execute();
    }

    public static void main(String[] args) throws IOException, ParseException {
        String serverUrl = "127.0.0.1:5000/";
        String baseUrl = "http://" + serverUrl;
//        RestClientSourceDatastore client =
//                new RestClientSourceDatastore(baseUrl, "table");

        // Create table1
        try (Response resp = post(baseUrl + "table1", "")) {
            assertEquals(201, resp.code());
        }

        // Check for table1
        try (Response resp = get(baseUrl + "table1")) {
            assertEquals(200, resp.code());
            String ct = resp.header("content-type");
            assertEquals("text/html; charset=utf-8", ct);
            ResponseBody body = resp.body();
            assertTrue(body != null && body.string().length() == 0);
        }

        // Check for non-existent table2
        try (Response resp = get(baseUrl + "table2")){
            assertEquals(404, resp.code());
        }

        // Check for docs in table1
        try (Response resp = get(baseUrl + "table1/docs")) {
            assertEquals(200, resp.code());
            String bodyStr = resp.body().string();
            String ct = resp.header("content-type");
            assertEquals("application/json", ct);
            JSONObject jo = (JSONObject) new JSONParser().parse(bodyStr);
            List<Document> docs = Document.fromDocumentsJson(
                    (JSONArray) jo.get("documents"));
            assertEquals(0, docs.size());
            assertEquals(0, (long) jo.get("current_sequence_id"));
        }

        // Put three docs in table1
        JSONObject d1 = new JSONObject() {{
            put(Document.ID, 1);
            put("var1", "value1");
        }};
        JSONObject d2 = new JSONObject() {{
            put(Document.ID, 2);
            put("var1", "value2");
        }};
        JSONObject d3 = new JSONObject() {{
            put(Document.ID, 3);
            put("var1", "value3");
        }};
        JSONArray data = new JSONArray() {{
            add(d1);
            add(d2);
            add(d3);
        }};
        try (Response resp = post(baseUrl + "table1/docs", data.toJSONString())) {
            assertEquals(200, resp.code());
            String bodyStr = resp.body().string();
            JSONObject jo = (JSONObject) new JSONParser().parse(bodyStr);
            assertEquals(3, (long) jo.get("num_docs_put"));
        }

        // Put the same three docs in table1, num_docs_put==0
        try (Response resp = post(baseUrl + "table1/docs", data.toJSONString())) {
            assertEquals(200, resp.code());
            String bodyStr = resp.body().string();
            JSONObject jo = (JSONObject) new JSONParser().parse(bodyStr);
            assertEquals(0, (long) jo.get("num_docs_put"));
        }

        // Check three docs in table1
        try (Response resp = get(baseUrl + "table1/docs")) {
            assertEquals(200, resp.code());
            String ct = resp.header("application/json");
            String bodyStr = resp.body().string();
            JSONObject jo = (JSONObject) new JSONParser().parse(bodyStr);
            JSONArray docs = (JSONArray) jo.get("documents");
            assertEquals(3, (long) docs.size());
            assertEquals(3, (long) jo.get("current_sequence_id"));
            // A server assigned revision numbers:
            assertEquals(1, (long) ((JSONObject)docs.get(0)).get(Document.REV));
            assertEquals(2, (long) ((JSONObject)docs.get(1)).get(Document.REV));
            assertEquals(3, (long) ((JSONObject)docs.get(2)).get(Document.REV));
        }

        System.out.println("SUCCESS");
        /*

    # Put docs in a local datastore
    ds = MemoryDatastore('datastore')
    d1a = {"_id": '1', "var1": "value1a"}
    d4 = {"_id": '4', "var1": "value4"}
    d5 = {"_id": '5', "var1": "value5"}
    for doc in [d1a, d4, d5]:
        ds.put(Document(doc))

    # Sync local datastore with remote table1
    remote_ds = RestClientSourceDatastore(base_url, 'table1')
    ds.sync_both_directions(remote_ds)

    # Check that table1 and table2 have the same things
    local_seq, local_docs = ds.get_docs_since(0, 10)
    remote_seq, remote_docs = remote_ds.get_docs_since(0, 10)
    assert local_seq == remote_seq
    assert len(local_docs) == len(remote_docs)
    for local_doc in local_docs:
        assert local_doc in remote_docs
    for remote_doc in remote_docs:
        assert remote_doc in local_docs
         */
    }
}
