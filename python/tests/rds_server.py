#!/usr/bin/env python3

from flask import Flask, request, abort, Response
# from reldatasync.datastore import PostgresDatastore
from reldatasync import util
from reldatasync.datastore import MemoryDatastore

import logging

from reldatasync.document import Document

datastores = {}


def _get_datastore(table, autocreate=True):
    if table not in datastores and autocreate:
        # datastores[table] = PostgresDatastore('datastore', _connstr(), table)
        datastores[table] = MemoryDatastore('server')
    return datastores.get(table, None)


def create_app():
    logging.info("SERVER STARTING")
    app = Flask(__name__)

# @app.route('/shutdown', methods=['GET'])
# def shutdown():
#     shutdown_server()
#     return 'Server shutting down...'

    @app.route('/')
    def hello():
        return {'datastores': [key for key in datastores.keys()]}

    # def _connstr():
    #     return ' '.join([
    #         "host=%s" % os.getenv('POSTGRES_HOST', 'db'),
    #         "user=%s" % os.getenv('POSTGRES_USER', 'postgres'),
    #         "dbname=%s" % os.getenv('POSTGRES_DB', 'test')])

    @app.route('/<table>', methods=['GET', 'POST'])
    def table_func(table):
        if request.method == 'POST':
            # Create table
            # If it already exists, that's okay
            _ = _get_datastore(table)
            # TODO: Return Location header of new resource
            # See also https://restfulapi.net/http-methods/#post
            return Response("", status=201)
        elif request.method == 'GET':
            if table not in datastores:
                abort(404)

        return ""

    @app.route('/<table>/sequence_id/<source>', methods=['GET'],
               defaults={'sequence_id': None})
    @app.route('/<table>/sequence_id/<source>/<sequence_id>', methods=['POST'])
    def sequence_id_func(table, source, sequence_id:int):
        datastore = _get_datastore(table, autocreate=False)
        if not datastore:
            abort(404)
        if request.method == 'GET':
            return {'sequence_id': datastore.get_peer_sequence_id(source)}
        elif request.method == 'POST':
            datastore.set_peer_sequence_id(source, sequence_id)
            return 'ok'

    @app.route('/<table>/docs', methods=['GET', 'POST'])
    def docs(table):
        datastore = _get_datastore(table, autocreate=False)
        if not datastore:
            abort(404)
        if request.method == 'GET':
            # return docs
            cur_seq_id, the_docs = datastore.get_docs_since(
                int(request.args.get('start_sequence_id', 0)),
                int(request.args.get('chunk_size', 10)))
            return {'current_sequence_id': cur_seq_id, 'documents': the_docs}
        elif request.method == 'POST':
            # put docs
            num_put = 0
            try:
                for the_doc in request.json:
                    increment_rev = (
                            request.args.get('increment_rev', False) == 'True')
                    num_put += datastore.put(
                        Document(the_doc), increment_rev=increment_rev)
            except ValueError as err:
                return str(err), 422
            # TODO: should response have docs with clocks set?  I think yes.
            return {'num_docs_put': num_put}

    @app.route('/<table>/doc/<docid>', methods=['GET'])
    @app.route('/<table>/doc', methods=['POST'],
               defaults={'docid': None})
    def doc(table, docid):
        datastore = _get_datastore(table, autocreate=False)
        if not datastore:
            abort(404)
        if request.method == 'GET':
            ret = datastore.get(docid)
            if not ret:
                abort(404)
            return ret
        elif request.method == 'POST':
            increment_rev = request.args.get('increment_rev', False) == 'True'
            num_put = datastore.put(
                Document(request.json), increment_rev=increment_rev)
            return {'num_docs_put': num_put}

    return app


if __name__ == '__main__':
    util.basic_config(logging.WARNING)

    app = create_app()
    app.run()
