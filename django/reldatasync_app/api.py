import json
import logging
from json import JSONDecodeError

from ninja import Field, NinjaAPI, Schema
from ninja.errors import HttpError
from reldatasync.datastore import Datastore, NoSuchTable
from reldatasync.document import Document
from reldatasync_app.models import DataSyncRevisions, SyncableModel

api = NinjaAPI()

logger = logging.getLogger(__name__)


def _get_datastore(datastore_name: str, table_name: str) -> Datastore:
    try:
        return SyncableModel.get_datastore_by_name(datastore_name, table_name)
    except DataSyncRevisions.DoesNotExist:
        raise HttpError(404, f"Datastore {datastore_name} not found")


class DatastoreSchema(Schema):
    id: str = Field(alias="datastore_id")
    name: str = Field(alias="datastore_name")


@api.get("", response=list[DatastoreSchema])
def datastores(request):
    return DataSyncRevisions.objects.all()


@api.get("{datastore}/{object_name}/doc/{docid}", response=dict)
def get_doc(
    request, datastore: str, object_name: str, docid: str, include_deleted: bool = False
):
    """GET a doc from datastore with the given docid.

    :param: `include_deleted`: if true, include deleted docs.  Default: false.
    """
    table = SyncableModel.get_table_by_class_name(object_name)
    if not table:
        raise HttpError(403, f"Unknown table '{object_name}'")
    with _get_datastore(datastore, table) as datastore1:
        ret = datastore1.get(docid, include_deleted=include_deleted)
        if not ret:
            raise HttpError(403, f"Doc '{docid}' not found")
        return ret


# TODO: Test passing increment_rev
@api.post("{datastore}/{object_name}/doc", response=dict)
def post_doc(request, datastore: str, object_name: str, increment_rev: bool = False):
    """POST a doc to the datastore.
    :param: `increment_rev`: if true, add a revision to the doc, otherwise fail if
       one is not present.  Default: false.

    :return `{"document": doc, "num_docs_put": <int>}`.
    """
    try:
        table = SyncableModel.get_table_by_class_name(object_name)
        if not table:
            raise HttpError(403, f"Unknown table '{object_name}'")
        with _get_datastore(datastore, table) as datastore1:
            # django-ninja can't parse because we don't have a schema
            data = _get_json_body(request)
            try:
                num_put, new_doc = datastore1.put(
                    Document(data), increment_rev=increment_rev
                )
            except ValueError as err:
                raise HttpError(422, str(err))
            return {"num_docs_put": num_put, "document": new_doc}
    except NoSuchTable:
        logger.warning(f"Table '{table}' not found")
        raise HttpError(404, f"No such table '{table}'")


def _get_json_body(request):
    """Get request body and return decoded JSON.

    Raise 422 if request.body cannot be parsed
    """
    the_body = request.body.decode("utf-8")
    try:
        data = json.loads(the_body)
    except JSONDecodeError:
        raise HttpError(422, f"Can't process body: {the_body}")
    return data


@api.get("{datastore}/{object_name}/docs", response=dict)
def get_docs(
    request,
    datastore: str,
    object_name: str,
    start_sequence_id: int,
    chunk_size: int = 100,
):
    """GET docs with `start_sequence_id < _seq <= (start_sequence_id+chunk_size)`

    Return `{"current_sequence_id": cur_seq_id, "documents": the_docs}`
    """
    table = SyncableModel.get_table_by_class_name(object_name)
    if not table:
        raise HttpError(403, f"Unknown table '{object_name}'")
    with _get_datastore(datastore, table) as datastore1:
        seq_id, docs = datastore1.get_docs_since(start_sequence_id, chunk_size)
        return {"current_sequence_id": seq_id, "documents": docs}


@api.post("{datastore}/{object_name}/docs", response=dict)
def put_docs(request, datastore: str, object_name: str, increment_rev: bool = False):
    """Put doc in given array of docs if rev is greater or doc doesn't exist.

    Return `{"num_docs_put": num_put, "documents": new_docs}`
    """
    table = SyncableModel.get_table_by_class_name(object_name)
    if not table:
        raise HttpError(403, f"Unknown table '{object_name}'")
    with _get_datastore(datastore, table) as datastore1:
        num_put = 0
        new_docs = []
        try:
            for the_doc in _get_json_body(request):
                num, new_doc = datastore1.put(
                    Document(the_doc), increment_rev=increment_rev
                )
                num_put += num
                if num:
                    new_docs.append(new_doc)
        except ValueError as err:
            raise HttpError(422, str(err))
        # TODO: should response have docs with clocks set?  I think yes.
        return {"num_docs_put": num_put, "documents": new_docs}
