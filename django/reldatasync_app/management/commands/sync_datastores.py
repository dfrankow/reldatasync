import logging
import sqlite3
from typing import Tuple
from urllib import parse
from urllib.parse import urlparse

import psycopg2
from django.core.management.base import BaseCommand
from reldatasync.datastore import Datastore, PostgresDatastore, SqliteDatastore
from reldatasync.replicator import Replicator

logger = logging.getLogger(__name__)


def break_url(ds_url: str) -> Tuple[str, str]:
    one, two = ds_url, ''
    qm = ds_url.find('?')
    if qm != -1:
        one, two = ds_url[:qm], ds_url[(qm+1):]
    return one, two


def get_datastore(ds_url: str) -> Datastore:
    # print(f'ds_url {ds_url}')
    dbc = urlparse(ds_url)
    # print(f'dbc {dbc}')

    if dbc.scheme in ['postgresql', 'sqlite']:
        params = parse.parse_qs(dbc.query)
        # print(f'dbc.params {dbc.params} params {params}')
        if not params.get('datastore', ''):
            raise ValueError(f'Missing datastore parameter in URL: {ds_url}')
        # logger.info(params)

        pathcomps = dbc.path.lstrip('/').split('/')
        # print(f'pathcomps {str(pathcomps)}')
        dbname, tablename = pathcomps

        if dbc.scheme == 'postgresql':
            # dbname/tablename?datastore=datastorename
            conn = psycopg2.connect(
                f'host={dbc.hostname}'
                f' user={dbc.username}'
                f' password={dbc.password}'
                f' dbname={dbname}')

            ds = PostgresDatastore(params['datastore'][0], conn, tablename)
        elif dbc.scheme == 'sqlite':
            # filename/tablename?datastore=datastorename
            conn = sqlite3.connect(dbname)
            ds = SqliteDatastore(params['datastore'][0], conn, tablename)
        else:
            raise ValueError('unknown datastore type {first_part}')
    else:
        raise ValueError(f'Unknown datastore URL {ds_url}')

    return ds


class Command(BaseCommand):
    help = 'Synchronize two datastores.'

    def add_arguments(self, parser):
        parser.add_argument('--ds1',
                            dest='ds1',
                            required=True,
                            help='Datastore 1')

        parser.add_argument('--ds2',
                            dest='ds2',
                            required=True,
                            help='Datastore 2')

    def handle(self, *args, **options):
        ds1_url = options.get('ds1', None)
        ds2_url = options.get('ds2', None)

        ds1 = get_datastore(ds1_url)
        ds2 = get_datastore(ds2_url)

        with ds1, ds2:
            Replicator(ds1, ds2).sync_both_directions()
            ds1.check()
            ds2.check()
            print(f'ds1 seq: {ds1.sequence_id}, ds2 seq: {ds2.sequence_id}')
