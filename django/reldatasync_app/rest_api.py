import logging
from typing import Type

from rest_framework.decorators import api_view
from rest_framework.exceptions import NotFound
from rest_framework.response import Response

from reldatasync.document import Document

from reldatasync_app.models import SyncableModel

logger = logging.getLogger(__name__)


_datastores = {}


def add_datastore_class(datastore_name, cls: Type[SyncableModel]):
    if datastore_name in _datastores:
        raise ValueError(f'Datastore {datastore_name} already in datastores')
    _datastores[datastore_name] = cls


def _get_datastore(datastore):
    if datastore not in _datastores:
        raise NotFound(f'Datastore {datastore} not found')
    cls = _datastores[datastore]

    return cls._get_datastore()


@api_view(['GET'])
def datastores(_request):
    return Response({'datastores': [key for key in _datastores.keys()]})


@api_view(['GET'])
def datastore_func(_request, datastore):
    with _get_datastore(datastore):
        # Return location of resource?
        return Response(status=200)


@api_view(['GET', 'POST'])
def sequence_id_func(
        request, datastore: str, source: str, sequence_id: int = None):
    with _get_datastore(datastore) as datastore:
        if request.method == 'GET':
            return Response(
                {'sequence_id': datastore.get_peer_sequence_id(source)})
        elif request.method == 'POST':
            # TODO: why allow this setting from the outside?
            datastore.set_peer_sequence_id(source, sequence_id)
            return Response('ok')


@api_view(['GET', 'POST'])
def docs(request, datastore):
    with _get_datastore(datastore) as datastore:
        if request.method == 'GET':
            # return docs
            start_sequence_id = int(
                request.query_params.get('start_sequence_id', 0))
            chunk_size = int(request.query_params.get('chunk_size', 10))

            # TODO(dan): Factor this out?
            cur_seq_id, the_docs = datastore.get_docs_since(
                start_sequence_id, chunk_size)
            return Response({'current_sequence_id': cur_seq_id,
                             'documents': the_docs})
        elif request.method == 'POST':
            # put docs
            increment_rev = request.query_params.get('increment_rev', False)
            # TODO(dan): Factor this out
            num_put = 0
            new_docs = []
            try:
                for the_doc in request.json:
                    num, new_doc = datastore.put(
                        Document(the_doc), increment_rev=increment_rev)
                    num_put += num
                    new_docs.append(new_doc)
            except ValueError as err:
                return Response(str(err), status=422)
            # TODO: should response have docs with clocks set?  I think yes.
            return Response({'num_docs_put': num_put,
                             'documents': new_docs})


# @app.route('/<datastore>/doc/<docid>', methods=['GET'])
# @app.route('/<datastore>/doc', methods=['POST'],
#            defaults={'docid': None})
@api_view(['GET', 'POST'])
def doc(request, datastore: str, docid: str = None):
    with _get_datastore(datastore) as datastore:
        if request.method == 'GET':
            ret = datastore.get(docid)
            if not ret:
                raise NotFound(f'Doc {docid} not found')
            return ret
        elif request.method == 'POST':
            # TODO(dan): Factor this out
            increment_rev = request.args.get('increment_rev', False) == 'True'
            num_put, new_doc = datastore.put(
                Document(request.json), increment_rev=increment_rev)
            return {'num_docs_put': num_put,
                    'document': new_doc}
