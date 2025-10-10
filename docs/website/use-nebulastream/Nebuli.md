---
title: "SQL REPL"
description: ""
lead: ""
date: 2020-10-13T15:21:01+02:00
lastmod: 2020-10-13T15:21:01+02:00
draft: false
menu:
  docs:
    parent: "use-nebulastream"
weight: 130
toc: true
---

In this section, we describe the usage of the SQL REPL frontend.
It allows for configuring and introspecting Nebulastream without writing YAML files, instead relying on SQL statements.

## Starting the REPL
To start it, run the nebuli binary without specifiying a further register/start/stop command.
If you just want to run nebulastream in a single-node setup, we recommend to use nebuli-embedded to start the system with an embedded query engine.
In that case, you do not need to pass connection details to a single-node-worker instance via the -s option.

## Using the REPL
If you start the repl in a TTY, you will see a welcome screen.
Statements are only registered after entering a newline after a semicolon after your statement.
If you running nebuli in non-interactive mode (for example by redirect a file into the stdin of nebuli), it will skip the welcome screen and only output the responses to your statements.

Nebuli supports rendering the responses of your statements as either text based columns, or json.
While some statements such as SHOW allow you to overwrite the format of the response, you can also specify it when starting nebuli with the -f option.

If you are using nebuli in non-interactive mode, it will stop the whole process after running all statements.
That means, if you are using nebuli-embedded, and you started some queries, the execution of the queries will also halt.
To prevent this, use the -w option to keep the process alive until all queries are done.

## Creating Sources and Sinks
We can create sources and sinks with `CREATE` statements.
Logical sources require a schema specification, while physical sources require a type and its corresponding configuration options.
Sinks require both, since there are no logical sinks.
We can use the standard SQL DDL syntax to define schemas, and in ABNF syntax `"TYPE" type_name` to define types, and `"SET" "(" *(option_value AS option_identifier) ")" `, similar to the projection syntax to set options.

As defined in the SQL standard, unquoted identifiers are implicitly upper-cased.
Option identifiers specified in snake case are automatically translated into camel case until they have been converted to snake case system-wide.


As an example, these statements create a logical sink my_sink, attach a file source to it that reads from a CSV file, and specify a sink that writes to a CSV file.
```sql
CREATE LOGICAL SOURCE testSource (timestamp UINT64, val INT32);
CREATE PHYSICAL SOURCE FOR testSource TYPE File SET ('/user/max/input.csv' as `SOURCE`.FILE_PATH, 'CSV' as PARSER.`TYPE`, '|' as PARSER.FIELD_DELIMITER, '\n' as PARSER.TUPLE_DELIMITER);
CREATE SINK test_sink (testSource.timestamp UINT64, testsource.val INT32) TYPE File SET ('/tmp/nebulastream/cmake-build-debug-docker/out.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT);
```
Creating physical sources returns an ID that we can later use to search for it or delete it.

## Running and Stopping Queries
We can start queries with SELECT statements as usual.

```sql
SELECT *
FROM TESTSOURCE
    WHERE val <= INT32(4)
INTO TEST_SINK;
```
Running the statement will return a query ID that we can use to search for it or delete it.

## Introspect Sources, Sinks, or Queries
To list all sources, sinks, or running queries, we can use `SHOW` statements.
The general syntax in ABNF is `"SHOW" ("LOGICAL SOURCES" / ("PHYSICAL SOURCES" *1("FOR" logical_source) / "SINKS" / "QUERIES") *1("WHERE" property "=" value) *1("FORMAT" ("TEXT" / "JSON"));`.

Where `property` can be 'id' for physical sources and queries, and 'name' for logical sources and sinks. 
The value must be an integer or a string literal, respectively.

In this example, the first query lists all physical sources for logical source my_source, and the second lists all queries and shows their status with id 3 in JSON format.

```sql
SHOW PHYSICAL SOURCES FOR my_source;
SHOW QUERIES WHERE id = 3 FORMAT JSON;
```


## Drop Sources, Sinks, or Queries
We can use the `DROP` statement to remove sources and sinks, or stop and unregister queries.
The syntax in ANBF is `"DROP" ("LOGICAL SOURCE" / "PHYSICAL SOURCE" / "SINK" / "QUERY") identifier;`
WHERE identifier is the name or ID of the logical source, sink, or physical source, query, respectively.

In this example, the first statement drops query 7, and the second drops logical source my_source and all its associated physical sources.
```sql
DROP QUERY 7;
DROP LOGICAL SOURCE my_source;
```
