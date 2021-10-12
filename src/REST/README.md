# REST APIs

Below we describe the REST APIs available for user to interact with system.
The NebulaStream REST API is versioned, with specific versions being queryable by prefixing the url with the version prefix. 
Prefixes are always of the form v[version_number]. For example, to access version 1 of /foo/bar one would query /v1/foo/bar.

Querying unsupported/non-existing versions will return a 404 exception.

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

*Submitting user query as a protobuf Object:*

**API**: /query/execute-query-ex \
**Verb**: POST \
**Response Code**: 200 OK

**_Example_**:

**Request**:
A Protobuf encoded QueryPlan, query String and PlacementStrategy.

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

To get all Logical streams registered at NebulaStream.

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

### Getting All Logical Stream for a Physical Stream
To get all Logical streams registered for a specific physical stream at NebulaStream.

**API**: /streamCatalog/allLogicalStream\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"physicalStreamName": "physical_stream_name"}

**Response**:
{[
"logical_stream_name": "logical_stream_schema"
]}

### Getting all Physical Stream

To get all physical streams.

**API**: /streamCatalog/allPhysicalStream\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{}

**Response**:
{"Physical Streams":  [physicl_stream_string]}

### Getting All Physical Stream For a Logical Stream

To get all physical streams for a given logical stream.

**API**: /streamCatalog/allPhysicalStream\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"logicalStreamName": "logical_stream_name"}

**Response**:
{"Physical Streams":  [physicl_stream_string]}

### Getting SourceConfig of Physical Stream
To get the source config of a physical stream

**API**: /streamCatalog/physicalStreamConfig\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"physicalStreamName": "physical_stream_name"}

**Response**:
{source_config_json}

### Getting all mismapped streams for Logical Stream
To get all physical streams which mapped to the specific logical stream without schema

**API**: /streamCatalog/allMismappedStreams\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"logicalStreamName": "logical_stream_name"}

**Response**:
{"Physical Streams":  ["phyStream1","phyStream2]}

### Getting all mismapped streams
To get all physical streams which mapped to a logical stream without schema

**API**: /streamCatalog/allMismappedStreams\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{}

**Response**:
{"Mismapped Streams":  ["mismappedLogical1":"phyStream1",
                        "mismappedLogical2":"phyStream1 \n phystream2"]}

### Getting all MISCONFIGURED physical streams
To get all physical streams which are labelled MISCONFIGURED (including why they are MISCONFIGURED)

**API**: /streamCatalog/allMisconfiguredStreamCatalogEntries\
**Verb**: GET\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{}

**Response**:
{"phyStream1": "stateDescriptionPhyStream1",
 "phyStream2": "stateDescriptionPhyStream2"}

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

### Add physical stream to logical stream
To add an existing physical to the logical-to-physical mapping of a logical stream

**API**: /streamCatalog/addPhysicalStreamToLogicalStream\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"logicalStreamName": "logical_stream_name",
"physicalStreamName": "phy1"}

**Response**:
{"Success": "true"}

### Activate duplicate physical stream name
If a physical stream tries to register with an already existing stream, a random string
is added at the end, and the physical stream labelled as MISCONFIGURED.
To be able to query on it, call this method to prove that the new name is valid

**API**: /streamCatalog/addPhysicalStreamToLogicalStream\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"physicalStreamName": "phy1_random"}

**Response**:
{"Success": "true"}

### Update Source Config of a Physical Stream
Used to update the configuration parameters of a Physical Streams Source Config

**API**: /streamCatalog/updatePhysicalStreamConfig\
**Verb**: POST\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"physicalStreamName": "phy1_random"} <- and params to update

**Response**:
{"Success": "true"}

### Delete Logical Stream
To delete a logical stream.

**API**: /streamCatalog/removeLogicalStream\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**: 

**Request**:
{"streamName": "logical_stream_name"}

**Response**:
{"Success": "true"}

### Remove a Physical Stream from a Logical-to-Physical mapping
To remove a Physical stream out of a logical-to-physical mapping

**API**: /streamCatalog/removePhysicalStreamFromLogicalStream\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"logicalStreamName": "logical_stream_name",
 "physicalStreamName": "physical_stream_name"}

**Response**:
{"Success": "true"}

### Remove a complete mismapped mapping (logical_without_schema to physical streams)
To remove an invalid mapping (logical stream has no schema but still physicals mapped 
to it)

**API**: /streamCatalog/removeMismappedForLogical\
**Verb**: DELETE\
**Response Code**: 200 OK

**_Example_**:

**Request**:
{"logicalStreamName": "logical_stream_name"}

**Response**:
{"Success": "true"}
