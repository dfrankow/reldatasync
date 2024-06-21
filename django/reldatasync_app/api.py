import logging

from ninja import Field, NinjaAPI, Schema
from reldatasync.datastore import Datastore
from reldatasync_app.models import DataSyncRevisions

api = NinjaAPI()

logger = logging.getLogger(__name__)


def _get_datastore(datastore_name: str) -> Datastore | tuple[int, dict]:
    result = None
    try:
        DataSyncRevisions.objects.get(datastore_name=datastore_name)
    except DataSyncRevisions.DoesNotExist:
        return 404, {"message": f"Datastore {datastore_name} not found"}

    return result


class DatastoreSchema(Schema):
    id: str = Field(alias="datastore_id")
    name: str = Field(alias="datastore_name")


@api.get("/", response=list[DatastoreSchema])
def datastores(request):
    return DataSyncRevisions.objects.all()


# TODO: Unit test
# @api.get("/{datastore}/doc/{docid}", response=dict)
# def doc(request, datastore: str, docid: int, include_deleted: bool = False):
#     """GET a doc from datastore with the given docid.
#
#     :param: `include_deleted`: if true, include deleted docs.  Default: false.
#     """
#     with _get_datastore(datastore) as datastore1:
#         if len(datastore1) == 2:
#             return datastore1
#
#         ret = datastore1.get(docid, include_deleted=include_deleted)
#         if not ret:
#             return 403, {"message": f"Doc {docid} not found"}
#         return ret


# TODO: Unit test
# @api.post("/{datastore}/doc", response=dict)
# def doc(request, datastore: str, increment_rev: bool = False):
#     """POST a doc to the datastore.
#     :param: `increment_rev`: if true, add a revision to the doc, otherwise fail if
#        one is not present.  Default: false.
#
#     :return `{"document": doc, "num_docs_put": <int>}`.
#     """
#     with _get_datastore(datastore) as datastore1:
#         if len(datastore1) == 2:
#             return datastore1
#
#         # TODO(dan): Factor this out
#         num_put, new_doc = datastore1.put(
#             Document(request.json), increment_rev=increment_rev
#         )
#         return {"num_docs_put": num_put, "document": new_doc}


# TODO: `/<datastore>/docs?start_sequence_id=<int>&chunk_size=<int>`
# GET docs put with `start_sequence_id < _seq <= (start_sequence_id+chunk_size)`
# Return `{"current_sequence_id": cur_seq_id, "documents": the_docs}`
#
# POST a json array of docs.
