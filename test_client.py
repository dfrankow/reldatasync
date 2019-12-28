#!/usr/bin/env python3

import requests


def server_url(url):
    return 'http://127.0.0.1:5000/' + url


def main():
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
    d1 = {"_id": 1, "var1": "value1"}
    d2 = {"_id": 2, "var1": "value2"}
    d3 = {"_id": 3, "var1": "value3"}
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

    # Create table2
    resp = requests.post(server_url('table2'))
    assert resp.status_code == 201

    # Put three docs in table1
    d1a = {"_id": 1, "var1": "value1a"}
    d4 = {"_id": 4, "var1": "value4"}
    d5 = {"_id": 5, "var1": "value5"}
    data = [d1a, d4, d5]
    resp = requests.post(server_url('table2/docs'), json=data)
    assert resp.status_code == 200
    ct = resp.headers['content-type']
    assert ct == 'application/json', f"content type '{ct}'"
    js = resp.json()
    assert js['num_docs_put'] == 3

    # Check docs in table2 and table1 are different
    docs1 = requests.get(server_url('table1/docs')).json()['documents']
    docs2 = requests.get(server_url('table2/docs')).json()['documents']
    assert docs1 != docs2
    d1a['_rev'] = 1
    d4['_rev'] = 2
    d5['_rev'] = 3
    assert d1a in docs2
    assert d1a not in docs1
    print(docs1)
    print(docs2)

    # TODO: Sync table2 with table1

    # TODO: Check that table1 and table2 have the same things


main()
