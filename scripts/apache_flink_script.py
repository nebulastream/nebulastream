import os
import yaml
import subprocess
import pandas as pd
import numpy as np
import tempfile
import re
import sys
import glob
from pyflink.common import Configuration
from pyflink.table import (
    TableEnvironment, EnvironmentSettings, DataTypes,
    TableDescriptor, Schema
)

# Mapping from YAML-specified Flink types to pyflink DataTypes.
FLINK_TYPE_MAP = {
    'INT': DataTypes.INT(),
    'BIGINT': DataTypes.BIGINT(),
    'DOUBLE': DataTypes.DOUBLE(),
    'FLOAT': DataTypes.FLOAT(),
    'STRING': DataTypes.STRING(),
    'BOOLEAN': DataTypes.BOOLEAN(),
    'SMALLINT': DataTypes.SMALLINT(),
    'TIMESTAMP(3)': DataTypes.TIMESTAMP(3),
    # Extend this map with additional Flink types as needed.
}


def map_dtype_to_flink(pd_dtype):
    """
    Map a pandas dtype to a corresponding Flink type string.
    """
    np_dtype = np.dtype(pd_dtype)
    if np.issubdtype(np_dtype, np.integer):
        if np_dtype == np.dtype('int32'):
            return "INT"
        elif np_dtype == np.dtype('int64'):
            return "BIGINT"
        else:
            return "INT"
    elif np.issubdtype(np_dtype, np.floating):
        if np_dtype == np.dtype('float32'):
            return "FLOAT"
        elif np_dtype == np.dtype('float64'):
            return "DOUBLE"
        else:
            return "FLOAT"
    elif np.issubdtype(np_dtype, np.bool_):
        return "BOOLEAN"
    elif np.issubdtype(np_dtype, np.datetime64):
        return "TIMESTAMP(3)"
    else:
        return "STRING"


def infer_schema_from_csv(csv_path, sample_size=100):
    """
    Infer schema from a CSV file using a sample of the data.
    Returns a list of field definitions with column name and type.
    """
    df = pd.read_csv(csv_path, nrows=sample_size)
    schema_fields = []
    for col in df.columns:
        flink_type_str = map_dtype_to_flink(df[col].dtype)
        field_def = {'name': col, 'type': flink_type_str}
        schema_fields.append(field_def)
        print(f"infered filed: {field_def}")
    return schema_fields


def create_tmp_csv_without_header(original_csv):
    """
    Create a temporary CSV file without headers.
    """
    df = pd.read_csv(original_csv)
    tmp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.csv')
    df.to_csv(tmp_file.name, index=False, header=False)
    return tmp_file.name


def is_timestamp_field(field):
    """
    Check if the field name suggests a timestamp column.
    """
    timestamp_keywords = {"ts", "timestamp", "eventTime", "time", "creationTS"}
    ts_pattern = re.compile(r".*TS$$")
    field_name = field.get("name")

    if field_name in timestamp_keywords or ts_pattern.match(field_name):
        if field_name.lower() == "timestamp":
            raise ValueError("Illegal column name 'timestamp'.")
        return True
    return False


def build_schema(fields, watermark_config=None):
    """
    Build a Flink Schema from a list of field definitions.
    Optionally add watermark configuration if provided.
    """
    schema_builder = Schema.new_builder()
    timestamp_column = None

    for col_def in fields:
        col_name = col_def['name']
        col_type_str = col_def.get('type')
        expression = col_def.get('expression')
        if is_timestamp_field(col_def):
            print(f"{col_name} was detected as ts field.")
            timestamp_column = col_name
            timestamp_expr = f"TO_TIMESTAMP_LTZ({col_name}, 3)"
            schema_builder.column_by_expression("eventTime", timestamp_expr)
            if col_type_str:
                flink_type = FLINK_TYPE_MAP.get(col_type_str.upper())
                if not flink_type:
                    raise ValueError(f"Unsupported Flink type: {col_type_str}")
                schema_builder.column(col_name, flink_type)
        elif expression:
            schema_builder.column_by_expression(col_name, expression)
        elif col_type_str:
            flink_type = FLINK_TYPE_MAP.get(col_type_str.upper())
            if not flink_type:
                raise ValueError(f"Unsupported Flink type: {col_type_str}")
            schema_builder.column(col_name, flink_type)
        else:
            raise ValueError(f"Field '{col_name}' must have either 'type' or 'expression' defined.")

    # If watermark configuration is provided and no timestamp field was detected,
    # add watermark using the configuration.
    if watermark_config and not timestamp_column:
        wm_column = watermark_config.get('column')
        wm_strategy = watermark_config.get('strategy')
    if timestamp_column:
        wm_column = "eventTime"
        wm_strategy = "eventTime"

        if wm_column and wm_strategy:
            schema_builder.watermark(wm_column, wm_strategy)
    return schema_builder.build()


class FlinkInstance:
    """
    Sets up a Flink TableEnvironment and manages table creation and query execution.
    """
    def __init__(self, parallelism=1):
        config = Configuration()
        config.set_integer("table.exec.resource.default-parallelism", parallelism)
        self.t_env = TableEnvironment.create(
            EnvironmentSettings.new_instance()
            .in_streaming_mode()
            .with_configuration(config)
            .build()
        )

    def create_input_table(self, table_name, table_config):
        """
        Create a temporary input table with the given name, reading from a CSV file.
        """
        csv_path = table_config['path']
        fields = table_config.get('fields')
        watermark_config = table_config.get('watermark')

        if not fields:
            print(f"[INFO] No fields provided for input table '{table_name}'. Inferring schema from CSV: {csv_path}")
            fields = infer_schema_from_csv(csv_path)
            csv_path = create_tmp_csv_without_header(csv_path)

        schema = build_schema(fields, watermark_config)
        descriptor = TableDescriptor.for_connector('filesystem') \
            .schema(schema) \
            .option('path', csv_path) \
            .format('csv') \
            .build()

        self.t_env.create_temporary_table(table_name, descriptor)
        print(f"[INFO] Created input table '{table_name}' from path: {csv_path}")
        print(f"[INFO] Schema: {schema}")

    def create_output_table(self, table_name, table_config):
        """
        Create a temporary sink (output) table with the given name, schema, and CSV path.
        """
        csv_path = table_config['path']
        fields = table_config.get('fields')

        # If the output path exists and is a directory (from a previous run), remove it to allow new output
        if os.path.exists(csv_path) and os.path.isdir(csv_path):
            import shutil
            print(f"[INFO] Output path '{csv_path}' is a directory. Removing it to allow new output.")
            shutil.rmtree(csv_path)

        if not fields:
            if os.path.exists(csv_path) and os.path.isfile(csv_path):
                print(f"[INFO] No fields provided for output table '{table_name}'. Inferring schema from CSV: {csv_path}")
                fields = infer_schema_from_csv(csv_path)
            else:
                query_prefix = (
                    f"CREATE TABLE {table_name} WITH "
                    f"('connector' = 'filesystem', 'path' = '{csv_path}', 'format' = 'csv') AS "
                )
                return query_prefix

        schema = build_schema(fields)
        descriptor = TableDescriptor.for_connector('filesystem') \
            .schema(schema) \
            .option('path', csv_path) \
            .format('csv') \
            .build()

        self.t_env.create_temporary_table(table_name, descriptor)
        print(f"[INFO] Created output table '{table_name}' at path: {csv_path}")
        print(f"[INFO] Schema: {schema}")

    def run_query(self, query_sql):
        """
        Execute a SQL query (e.g., INSERT INTO sink_table SELECT ...).
        """
        job_client = self.t_env.execute_sql(query_sql)
        job_client.wait()
        print("[INFO] Query finished.")


class QueryRun:
    """
    Manages configuration loading, table setup, query execution, and optional post-processing.
    """
    def __init__(self, flink_instance, config_path, input_csv=None):
        self.flink_instance = flink_instance
        self.config_path = config_path
        self.input_csv = input_csv
        self.config = None
        self.query_output_table_prefixes = []
        self.output_tables = []

    def load_config(self):
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        print(f"[INFO] Loaded config from {self.config_path}")

    def setup_tables(self):
        # Set up input tables.
        for tbl in self.config.get('input_tables', []):
            table_name = tbl['logicalSourceName']
            self.flink_instance.create_input_table(table_name, tbl)

        # Set up output tables.
        for tbl in self.config.get('output_tables', []):
            table_name = tbl['name']
            self.output_tables.append(tbl['path'])
            prefix = self.flink_instance.create_output_table(table_name, tbl)
            if prefix:
                self.query_output_table_prefixes.append(prefix)

    def run_queries(self):
        queries = self.config.get('queries', [])
        # Combine output table prefixes with queries if applicable.
        combined_queries = [
            prefix + query for prefix, query in zip(self.query_output_table_prefixes, queries)
        ]
        for query in combined_queries:
            print(f"[INFO] Running query:\n {query}")
            try:
                self.flink_instance.run_query(query)
            except Exception as e:
                print(f"[ERROR] Query failed: {e}")

    def call_external_script(self):
        script_path = self.config.get('checksum_script')
        if not script_path:
            print("[WARN] No 'checksum_script' specified in the config.")
            return

        for output_dir in self.output_tables:
            file_pattern = os.path.join(output_dir, '*')
            output_files = glob.glob(file_pattern)

            if output_files:
                output_file = output_files[0]
                script_cmd = f"{script_path} < {output_file}"
                print(script_cmd)
                if script_cmd:
                    print(f"\n[INFO] Calling external script: {script_cmd}")
                    result = subprocess.run(script_cmd, shell=True, capture_output=True, text=True)
                    print("STDOUT:")
                    print(result.stdout)
                    if result.stderr:
                        print("STDERR:")
                        print(result.stderr)

    def execute(self):
        self.load_config()
        self.setup_tables()
        self.run_queries()
        self.call_external_script()


def main(config_path, input_csv=None, parallelism=1):
    flink_instance = FlinkInstance(parallelism=parallelism)
    query_run = QueryRun(flink_instance, config_path, input_csv)
    query_run.execute()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Flink Queries via Python.")
    parser.add_argument("--config", required=True, help="Path to the YAML config file.")
    parser.add_argument("--input_csv", default=None, help="Path to the input CSV file (if not specified in config).")
    parser.add_argument("--parallelism", default=1, type=int, help="Parallelism level for Flink.")
    args = parser.parse_args()

    main(args.config, args.input_csv, args.parallelism)