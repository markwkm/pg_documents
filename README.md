# pg_documents

The pg_documents module provides an HTTP daemon and was developed using json-c
v0.11.

## Configuration Paramters

pg_documents.database

    Name of database to connect to.

pg_documents.max_sockets

    Maximum number of connections.

pg_documents.port

    Port to listen to for HTTP requests.

pg_documents.queue_depth

    Queue size for HTTPD listener.

## HTTP API Reference

### Table Methods

#### DELETE /tablename

Drops a table.

#### GET /_all_dbs

List all tables in the database.

#### PUT /tablename

Creates a new table for storing documents.

### Document methods

#### DELETE /tablename/id

Delete a document by id.

#### GET /tablename/id

Retrieve a document by id.

#### POST /tablename

Creates a new document with a randomly assign id.

#### PUT /tablename/id

Creates or update a document with the specific id.
