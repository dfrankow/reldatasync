from django.urls import path

from reldatasync_app import rest_api

urlpatterns = [
    path('', rest_api.datastores, name='datastores'),
    path('<datastore>', rest_api.datastore_func, name='datastore'),
    path('<datastore>/sequence_id/<source>',
         rest_api.sequence_id_func, name='sequence_id'),
    path('<datastore>/sequence_id/<source>/<int:sequence_id>',
         rest_api.sequence_id_func, name='sequence_id_with_id'),

    path('<datastore>/docs', rest_api.docs, name='docs'),
    path('<datastore>/doc', rest_api.doc, name='doc'),
    path('<datastore>/doc/<docid>', rest_api.doc, name='doc_with_id'),
]
