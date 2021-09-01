package org.maventy.reldatasync;

public class TestClient {
    public static void main() {
        String serverUrl = "0.0.0.0:5000/";
        String baseUrl = "http://" + serverUrl;
        RestClientSourceDatastore client =
                new RestClientSourceDatastore(baseUrl, "table");

        /*
            # Create table1
        resp = requests.post(server_url('table1'))
        assert resp.status_code == 201
         */
        resp = post(baseUrl + "table1");

        /*

def main():
    parser = argparse.ArgumentParser(description='Test REST server.')
    parser.add_argument('--server-url', '-s', dest='server_url',
                        required=True,
                        help='URL of the server')
    args = parser.parse_args()
    base_url = "http://" + args.server_url
    # server_url is a function that returns base + rest
    server_url = lambda url: base_url + url

    # Create table1
    resp = requests.post(server_url('table1'))
    assert resp.status_code == 201

    # Check for table1
    resp = requests.get(server_url('table1'))
    assert resp.status_code == 200
    ct = resp.headers['content-type']
    assert ct == 'text/html; charset=utf-8', f"content type '{ct}'"
    assert resp.text == ""

    # Check for non-existent table2
    resp = requests.get(server_url('table2'))
    assert resp.status_code == 404

    # Check for docs in table1
    resp = requests.get(server_url('table1/docs'))
    assert resp.status_code == 200
    ct = resp.headers['content-type']
    assert ct == 'application/json', f"content type '{ct}'"
    js = resp.json()
    assert js['documents'] == []
    assert js['current_sequence_id'] == 0

    # Put three docs in table1
    d1 = {"_id": '1', "var1": "value1"}
    d2 = {"_id": '2', "var1": "value2"}
    d3 = {"_id": '3', "var1": "value3"}
    data = [d1, d2, d3]
    resp = requests.post(server_url('table1/docs'), json=data)
    assert resp.status_code == 200
    ct = resp.headers['content-type']
    assert ct == 'application/json', f"content type '{ct}'"
    js = resp.json()
    assert js['num_docs_put'] == 3

    # Put the same three docs in table1, num_docs_put==0
    resp = requests.post(server_url('table1/docs'), json=data)
    assert resp.status_code == 200
    js = resp.json()
    assert js['num_docs_put'] == 0

    # Check three docs in table1
    resp = requests.get(server_url('table1/docs'))
    assert resp.status_code == 200
    ct = resp.headers['content-type']
    assert ct == 'application/json', f"content type '{ct}'"
    js = resp.json()
    assert len(js['documents']) == 3, f'js is {js}'
    assert js['current_sequence_id'] == 3, f'js is {js}'
    # server assigned revision numbers:
    d1['_rev'] = 1
    d2['_rev'] = 2
    d3['_rev'] = 3
    docs = js['documents']
    assert d1 in docs
    assert d2 in docs
    assert d3 in docs

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
