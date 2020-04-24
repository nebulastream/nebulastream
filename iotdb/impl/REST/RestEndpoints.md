# REST APIs

Below we describe the REST APIs available for user to interact with system.
The NebulaStream REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. 
Prefixes are always of the form v[version_number]. For example, to access version 1 of /foo/bar one would query /v1/foo/bar.

Querying unsupported/non-existing versions will return a 404 error.

There exist several async operations among these APIs, e.g. submit a job. These async calls will return a triggerid to 
identify the operation you just POST and then you need to use that triggerid to query for the status of the operation.
___

## Query 
Here we describe the available endpoints used for submitting and interacting with a user query.
 
### Submitting User Query

Submitting user query for execution.
 
**API**: /query/execute-query \
**Verb**: POST \
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);", "strategyName": "BottomUp"}

**Response**:
{"queryId": "system_generate_uuid"}

### Getting Execution Plan

Getting the execution plan for the user query.
 
**API**: /query/execution-plan\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);", "strategyName": "BottomUp"}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeType": "node_type",
        "capacity": "node_capacity",
        "remainingCapacity": "remaining_capacity",
        "physicalStreamName": "physical_stream_name"
    }],
"edges": [{
        "source":"source_node",    
        "target":"target_node",    
        "linkCapacity":"link_capacity",    
        "linkLatency":"link_latency",    
    }]
}

### Get Query plan

Get query plan for the user query.
 
**API**: /query/query-plan\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"userQuery":"InputQuery::from(temperature).print(std::cout);"}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeType": "node_type"
    }],
"edges": [{
        "source":"source_operator",    
        "target":"target_operator"
    }]
}

### Getting NebulaStream Topology graph

To get the NebulaStream topology graph as JSON.

**API**: /query/nes-topology\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{"nodes": [{
        "id": "node_id",
        "title": "node_title",
        "nodeType": "node_type",
        "capacity": "node_capacity",
        "remainingCapacity": "remaining_capacity",
        "physicalStreamName": "physical_stream_name"
    }],
"edges": [{
        "source":"source_node",    
        "target":"target_node",    
        "linkCapacity":"link_capacity",    
        "linkLatency":"link_latency",    
    }]
}

___

## Query Catalog

Here we describe the APIs used for interacting with query catalog.

### Getting All Queries

To get all queries registered at NebulaStream.

**API**: /queryCatalog/allRegisteredQueries\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{[
"system_generated_query_id": "query_string" 
]}

### Getting Queries With Status

To get all queries with a specific status form NebulaStream.

API: /queryCatalog/queries\
Verb: GET\
Response Code: 200 OK

**_Example_**: 

**Request**:
{"status":"Running"}

**Response**:
{[
"system_generated_query_id": "query_string" 
]}

### Delete User Query

To delete a user submitted query.

**API**: /queryCatalog/query\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"queryId": "system_generate_uuid"}

**Response**:
{}
___

## Stream Catalog

### Getting All Logical Stream

To get all queries registered at NebulaStream.

**API**: /streamCatalog/allLogicalStream\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{}

**Response**:
{[
"logical_stream_name": "logical_stream_schema" 
]}

### Getting All Physical Stream For a Logical Stream

To get all physical streams for a given logical stream.

**API**: /streamCatalog/allPhysicalStream\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"streamName": "logical_stream_name"}

**Response**:
{"Physical Streams":  [physicl_stream_string]}

### Add Logical Stream
To add a logical stream.

**API**: /streamCatalog/addLogicalStream\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"streamName": "logical_stream_name",
"schema": "Schema::create()->addField(\"test\",INT32);"}

**Response**:
{"Success": "true"}

### Update Logical Stream
To Update a logical stream.

**API**: /streamCatalog/updateLogicalStream\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"streamName": "logical_stream_name",
"schema": "Schema::create()->addField(\"test\",INT32);"}

**Response**:
{"Success": "true"}

### Delete Logical Stream

To delete a logical stream.

**API**: /streamCatalog/deleteLogicalStream\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"streamName": "logical_stream_name"}

**Response**:
{"Success": "true"}
