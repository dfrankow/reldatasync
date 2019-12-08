#!/usr/bin/env python3

from flask import Flask, request
# from datastore import PostgresDatastore
from .datastore import MemoryDatastore

app = Flask(__name__)


datastores = {}


@app.route('/')
def hello():
    return 'Hello'


# def _connstr():
#     return ' '.join([
#         "host=%s" % os.getenv('POSTGRES_HOST', 'db'),
#         "user=%s" % os.getenv('POSTGRES_USER', 'postgres'),
#         "dbname=%s" % os.getenv('POSTGRES_DB', 'test')])


def _get_datastore(table):
    if table not in datastores:
        # datastores[table] = PostgresDatastore('datastore', _connstr(), table)
        datastores[table] = MemoryDatastore('datastore')
    return datastores[table]


@app.route('/<table>', methods=['GET'])
def table_func(table):
    if request.method == 'GET':
        return table in datastores


@app.route('/<table>/sequence_id/<source>', methods=['GET'],
           defaults={'sequence_id': None})
@app.route('/<table>/sequence_id/<source>/<sequence_id>', methods=['POST'])
def sequence_id_func(table, source, sequence_id:int):
    datastore = _get_datastore(table)
    if request.method == 'GET':
        return {'sequence_id': datastore.get_peer_sequence_id(source)}
    elif request.method == 'POST':
        datastore.set_peer_sequence_id(source, sequence_id)
        return 'ok'


@app.route('/<table>/docs', methods=['GET', 'POST'])
def docs(table):
    datastore = _get_datastore(table)
    if request.method == 'GET':
        # return docs
        cur_seq_id, docs = datastore.get_docs_since(
            request.args.get('start_sequence_id', 1),
            request.args.get('chunk_size', 10))
        return {'current_sequence_id': cur_seq_id, 'documents': docs}
    elif request.meothd == 'POST':
        # put docs
        num_put = 0
        for doc in request.json:
            num_put += datastore.put_if_needed(doc)
        return {'num_docs_put': num_put}
