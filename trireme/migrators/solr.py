from __future__ import absolute_import
from invoke import task
from requests.auth import HTTPBasicAuth
import requests
import os
from config import solr_url, username, password, keyspace

auth = HTTPBasicAuth(username, password)


def upload_file(local_path, remote_path):
    fd = open(local_path, 'r')
    response = requests.post(remote_path, data=fd, auth=auth)
    fd.close()
    return response


def find_tables():
    tables = []
    tables_path = 'db/solr'

    potential_tables = os.listdir(tables_path)
    for potential_table in potential_tables:
        # Ignore any non-directory files in here
        if os.path.isdir("{}/{}".format(tables_path, potential_table)):
            tables.append(potential_table)
    return tables


def _core_name(table):
    core = "%s.%s" % (keyspace, table)
    return core


@task(help={'table': 'Name of the table to use to create the core. Omitting this value will create cores for all tables'})
def create(table=None):
    tables = []
    if table:
        tables.append(table)
    else:
        tables = find_tables()

    for table in tables:
        core = _core_name(table)
        print("Creating Core {}".format(core))

        core_files = os.listdir("db/solr/{}".format(table))
        for core_file in core_files:
            print("Uploading {} to {}".format(core_file, core))
            response = upload_file("db/solr/{}/{}".format(table, core_file), "{}/resource/{}/{}".format(solr_url, core,
                                                                                                       core_file))
            if response.status_code == 200:
                print('SUCCESS')
            else:
                raise RuntimeError("Error uploading {}".format(core_file))

        response = requests.get("{}/admin/cores?action=CREATE&name={}".format(solr_url, core), auth=auth)
        if response.status_code == 200:
            print('Core created, you may view the status in the web interface')


@task(help={'table': 'Name of the table to use to create the core. Omitting this value will create cores for all tables'})
def migrate(table=None):
    tables = []
    if table:
        tables.append(table)
    else:
        tables = find_tables()

    for table in tables:
        core = _core_name(table)
        print("Updating Core {}".format(core))

        core_files = os.listdir("db/solr/{}".format(table))
        for core_file in core_files:
            print("Uploading {} to {}".format(core_file, core))
            response = upload_file("db/solr/{}/{}".format(table, core_file), "{}/resource/{}/{}".format(solr_url, core,
                                                                                                       core_file))
            if response.status_code == 200:
                print('SUCCESS')
            else:
                raise RuntimeError("Error uploading {}".format(core_file))

        print('Reloading core')
        response = requests.get("{}/admin/cores?action=RELOAD&name={}".format(solr_url, core), auth=auth)
        if response.status_code == 200:
            print('Successfully reloaded Solr core')


@task(help={'name': 'Name of the table you want to create core for'})
def add_table(name):
    if name:
        path = "db/solr/{}".format(name)
        if os.path.exists(path):
            print("File or directory: {} already exists. Aborting.".format(path))
        else:
            print("Creating directory {}".format(path))
            os.makedirs(path)

            print('Creating EMPTY solrconfig.xml')
            fd = open("{}/solrconfig.xml".format(path), 'w')
            fd.close()

            print('Creating EMPTY schema.xml')
            fd = open("{}/schema.xml".format(path), 'w')
            fd.close()
    else:
        print('Call add_table with the --name parameter specifying a name. Ex: bar - where bar is the table name specified for the keyspace in config.py')
