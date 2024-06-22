import logging

from ninja import Field, NinjaAPI, Schema
from ninja.errors import HttpError
from reldatasync.datastore import Datastore
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


class ErrorSchema(Schema):
    message: str


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


# TODO: Unit test
@api.post("{datastore}/{table}/doc", response=dict)
def post_doc(request, datastore: str, table: str, increment_rev: bool = False):
    """POST a doc to the datastore.
    :param: `increment_rev`: if true, add a revision to the doc, otherwise fail if
       one is not present.  Default: false.

    :return `{"document": doc, "num_docs_put": <int>}`.
    """
    with _get_datastore(datastore, table) as datastore1:
        # TODO(dan): Factor this out
        num_put, new_doc = datastore1.put(
            Document(request.json), increment_rev=increment_rev
        )
        return {"num_docs_put": num_put, "document": new_doc}


# TODO: `/<datastore>/docs?start_sequence_id=<int>&chunk_size=<int>`
# GET docs put with `start_sequence_id < _seq <= (start_sequence_id+chunk_size)`
# Return `{"current_sequence_id": cur_seq_id, "documents": the_docs}`
#
# POST a json array of docs.
