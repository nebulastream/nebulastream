# Flink Query Runner

## Overview

The **Flink Query Runner** is a Python script that automates data processing using Apache Flink. It sets up a Flink `TableEnvironment`, reads input data from CSV files defined in a YAML configuration file, and executes SQL queries on that data. Additionally, the script supports outputting query results to CSV files and optionally executing an external checksum script for result verification.

## Features

- **Input Table Setup**  
  Reads CSV files as input tables. If schema details (fields) are not provided, the script automatically infers them from a sample of the CSV data.

- **Output Table Setup**  
  Configures output (sink) tables where query results are stored as CSV files. Schema details can be specified or automatically written to the specified path.

- **SQL Query Execution**  
  Executes one or more SQL queries provided in the configuration file. For output tables without a pre-defined schema, the script can automatically prepend a table creation query.

- **Schema Inference**  
  Automatically deduces the schema for input tables by sampling CSV data and mapping pandas data types to corresponding Flink types.

- **Watermark Configuration**  
  Supports optional watermark configuration for event time processing, ensuring proper handling of time-based operations. The orginal Time value of the data should contain TS so the script can detect it as the time column.

- **External Script Execution**  
  After running queries, the script can invoke an external checksum script on each output file to validate the results.

## Requirements

- Python 3.x
- PyFlink, Pandas, NumPy, PyYAML
- An Apache Flink runtime environment

---

## Configuration File Options

The **YAML configuration file** provides the necessary settings for input tables, output tables, queries, and external scripts. Below is an explanation of each section:

### **`input_tables`**

Defines one or more input tables. Each table configuration should include:

- **`logicalSourceName`**  
  A unique identifier for the input table. This name is used to reference the table in SQL queries.

- **`path`**  
  The absolute path to the CSV file containing the input data.

- **`fields` (optional)**  
  A list of field definitions (columns) that includes the column name and the data type. If omitted, the schema is inferred automatically from the CSV file.

- **`watermark` (optional)**  
  Configuration for event time watermarks. It typically includes:
    - `column`: The name of the timestamp column.
    - `strategy`: The watermark strategy (e.g., a delay expression).

### **`output_tables`**

Defines one or more output (sink) tables where query results are written. Each table configuration should include:

- **`name`**  
  A unique identifier for the output table.

- **`path`**  
  The directory where the output CSV files will be stored.

- **`fields` (optional)**  
  Similar to input tables, these define the schema for the output. If absent, the script may attempt to infer the schema from an existing CSV file in the specified path.

### **`queries`**

A list of SQL queries to execute. Each query is written in standard SQL syntax and can reference any input table defined in the configuration. Example:

```yaml
queries:
  - >
    SELECT * FROM consumers WHERE consumedPower >= 400;
  - >
    SELECT consumerId, eventTime
    FROM factories
    WHERE eventTime > TIMESTAMP '2024-01-21 21:12:00';
```

### **`checksum_script`**

Specifies the file path to an external checksum script. After the queries are executed, the script will be called for each output directory to verify the integrity of the resulting CSV files. The command is executed in the form:

```
/path/to/checksum_script < output_file.csv
```

### **`post_processing_script` (Optional)**

This option is currently commented out but may be used to specify a script for additional post-processing of query outputs.

---

## How to Use

### **1. Prepare the Configuration File**

Create a YAML configuration file (e.g., `config.yml`) with your specific settings. An example configuration is provided below:

```yaml
input_tables:
  - logicalSourceName: "consumers"
    path: "/home/tim/Documents/work/apache_flink/data/IoTropolis_Output/CONSUMERS_TOPIC.csv"
  
  - logicalSourceName: "solarPanels"
    path: "/home/tim/Documents/work/apache_flink/data/IoTropolis_Output/SOLAR_PANELS_TOPIC.csv"
  
  - logicalSourceName: "factories"
    path: "/home/tim/Documents/work/apache_flink/data/IoTropolis_Output/FACTORIES_TOPIC.csv"

output_tables:
  - name: "sink_q1"
    path: "/home/tim/Documents/work/apache_flink/data/IoTropolis_Output/small/outputs/test/sink_q1"
  
  - name: "sink_q_test"
    path: "/home/tim/Documents/work/apache_flink/data/IoTropolis_Output/small/outputs/test/sink_test"

queries:
  - >
    SELECT * FROM consumers WHERE consumedPower >= 400;
  - >
    SELECT consumerId, eventTime
    FROM factories
    WHERE eventTime > TIMESTAMP '2024-01-21 21:12:00';

checksum_script: "/home/tim/Documents/work/nebulastream-public/cmake-build-debug-docker-code-coverage/nes-systests/systest/checksum/checksum"
```

### **2. Run the Script**

Execute the script from the command line:

```
python your_script.py --config path/to/config.yml
```

Additional optional arguments:

- **`--input_csv`**: Provide a path to override the input CSV if not specified in the config.
- **`--parallelism`**: Set the parallelism level for Flink (default is `1`).

Example with parallelism:

```
python your_script.py --config config.yml --parallelism 4
```

### **3. Monitor the Process**

The script prints informative log messages:

- It indicates when input and output tables are created.
- It logs schema inference details if the schema is not provided.
- It displays the SQL queries being executed.
- It shows the execution of the external checksum script (if specified).

### **4. Post-processing**

If you decide to use a post-processing script (by uncommenting or adding the `post_processing_script` option in your configuration), the script can further process the output CSV files as needed.

---

## Final Notes

- **File Paths**  
  Ensure all file paths specified in the configuration file are correct and accessible.

- **Error Handling**  
  The script will infer schemas when not provided but will raise errors if required fields (like a proper timestamp) do not meet the naming conventions or if unsupported Flink types are specified.

- **Customization**  
  Feel free to modify or extend the script to suit your specific requirements. This tool is designed to be flexible and can be adapted for a variety of data processing tasks.
