import json
import os
import configparser
import time
import warnings
from datetime import datetime
import logging
from collections import defaultdict

from sqlalchemy import create_engine, inspect, text, exc as sa_exc
from sqlalchemy.engine import URL

config = configparser.ConfigParser()
config.read('config.ini')

# Accumulators for error messages
error_tables = defaultdict(list)
error_views = defaultdict(list)

TYPE = None
# warnings.filterwarnings('ignore', category=sa_exc.SAWarning, message='Did not recognize type')


def log_errors():
    if error_tables:
        logging.error("Error retrieving Tables:")
        for schema, tables in error_tables.items():
            for table in tables:
                logging.error(f"{table}")

    if error_views:
        logging.error("Error retrieving Views:")
        for schema, views in error_views.items():
            for view in views:
                logging.error(f"{view}")


try:

    # Add the DB2 driver path
    os.add_dll_directory(
        'C:\\Users\\MITDeepanraj\\PycharmProjects\\schemaValidator\\.venv\\Lib\\site-packages\\clidriver\\bin')

    # Read configuration file

    source = config['COMPARISON']['SOURCE']
    target = config['COMPARISON']['TARGET']

    # Database connection details for Source DB
    source_db = URL.create(
        drivername=config[source]['driver'],
        username=config[source]['username'],
        password=config[source]['password'],
        host=f"{config[source]['host']}:{config[source]['port']}/{config[source]['service_name']}"
    )

    # Database connection details for Target DB
    target_db = URL.create(
        drivername=config[target]['driver'],
        username=config[target]['username'],
        password=config[target]['password'],
        host=f"{config[source]['host']}:{config[source]['port']}/{config[source]['service_name']}"
    )

    # Create engines
    source_engine = create_engine(source_db)
    target_engine = create_engine(target_db)

    # Inspectors
    source_inspector = inspect(source_engine)
    target_inspector = inspect(target_engine)


except (configparser.Error, KeyError) as config_error:
    print(f"Configuration error: {config_error}")
    exit(1)
except Exception as e:
    print(f"Failed to set up database connections: {e}")
    exit(1)


def get_trigger_schema(engine, schema_name, trigger_name):
    try:
        query = text("""
            SELECT NAME, TEXT
            FROM SYSIBM.SYSTRIGGERS
            WHERE SCHEMA = :schema_name
            AND TRIGNAME = :trigger_name;
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name, 'trigger_name': trigger_name})
            row = result.fetchone()
            if row:
                return {
                    trigger_name: {
                        "definition": row[1]
                    }
                }
            return {}
    except Exception as e:
        print(f"Error retrieving trigger schema: {e}")
        return {}


def get_triggers(inspector, schema_name, table_name):
    try:
        query = text("""
            SELECT NAME, TEXT FROM SYSIBM.SYSTRIGGERS WHERE SCHEMA = :schema_name;
        """)
        with inspector.engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name, 'table_name': table_name})
            triggers = {}
            for row in result:
                triggers[row[0]] = row[1]  # Trigger name and trigger definition
            print(triggers)
        return triggers
    except Exception as e:
        print(f"Error retrieving triggers: {e}")
        return {}


def get_table_schema(inspector, schema_name, table_name, type):
    global TYPE
    TYPE = type
    try:
        columns = inspector.get_columns(table_name, schema=schema_name)
        # print(columns)
        primary_keys = inspector.get_pk_constraint(table_name, schema=schema_name)
        foreign_keys = inspector.get_foreign_keys(table_name, schema=schema_name)
        unique_constraints = inspector.get_unique_constraints(table_name, schema=schema_name)
        # indexes = inspector.get_indexes(table_name, schema=schema_name)  # Fetch indexes
        # triggers = get_triggers(inspector, schema_name, table_name)  # Fetch triggers

        schema = {}

        # Add column information
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            column_type_str = str(column_type).upper()
            column_length = column_type.length if hasattr(column_type, 'length') else None
            column_data = {
                "datatype": column_type_str
            }
            if column_length:
                if isinstance(column_length, str):
                    length_info = column_length.strip()
                elif isinstance(column_length, int):
                    length_info = str(column_length)
                else:
                    length_info = None

                if length_info:
                    if "," in length_info:
                        precision, scale = length_info.split(",")
                        column_data["precision"] = int(precision.strip())
                        column_data["scale"] = int(scale.strip())
                    else:
                        column_data["length"] = int(length_info.strip())

            # Add default value if available
            if column.get('default'):
                column_data['default'] = column['default']

            # Add nullability
            column_data['is_nullable'] = column['nullable']

            schema[column_name] = column_data

        # Add primary key constraint
        if primary_keys:
            schema['primary_key'] = primary_keys['constrained_columns']

        # Add foreign key constraints
        if foreign_keys:
            schema['foreign_keys'] = []
            for fk in foreign_keys:
                schema['foreign_keys'].append({
                    'column': fk['constrained_columns'],
                    'referenced_table': fk['referred_table'],
                    'referenced_columns': fk['referred_columns']
                })

        # Add unique constraints
        if unique_constraints:
            schema['unique_constraints'] = [uc['column_names'] for uc in unique_constraints]

        # Add check constraints with a fallback
        try:
            check_constraints = inspector.get_check_constraints(table_name, schema=schema_name)
            print("Check constraint data:", check_constraints)
            if check_constraints:
                schema['check_constraints'] = [cc['sqltext'] for cc in check_constraints]
        except NotImplementedError:
            # Fallback mechanism
            try:
                # Adjusted SQL query for DB2
                query = text("""
                    SELECT C.NAME, C.TEXT
                    FROM SYSIBM.SYSCHECKS C
                    JOIN SYSIBM.SYSTABLES T ON C.TBCREATOR = T.CREATOR AND C.TBNAME = T.NAME
                    WHERE C.TBNAME = :table_name
                    AND C.TBCREATOR = :schema_name
                """)
                with inspector.engine.connect() as conn:
                    result = conn.execute(query, {'schema_name': schema_name, 'table_name': table_name})
                    check_clauses = [row[1] for row in result]  # Ensure to handle possible None values
                    if check_clauses:
                        schema['check_constraints'] = check_clauses
            except Exception as e:
                print(f"Error retrieving check constraints: {e}")

            # Add indexes
            # if indexes:
            #     schema['indexes'] = {idx['name']: idx['column_names'] for idx in indexes}

            # Add triggers
            # if triggers:
            #     schema['triggers'] = triggers

        return schema
    except Exception as e:
        error_tables[TYPE].append(f"{TYPE} - {schema_name}.{table_name}")
        return {}


def get_view_schema(inspector, schema_name, view_name, type):
    global TYPE
    TYPE = type
    try:
        # For views, we might not need detailed schema, but let's fetch columns as example
        columns = inspector.get_columns(view_name)
        schema = {}
        for column in columns:
            column_name = column['name']
            column_type = column['type']
            column_type_str = str(column_type).upper()
            schema[column_name] = {
                "datatype": column_type_str
            }
        return schema
    except Exception as e:
        error_views[TYPE].append(f"{TYPE} - {schema_name}.{view_name}")
        return {}


def get_function_schema(engine, schema_name, function_name):
    try:
        query = text("""
            SELECT  NAME, Body
                FROM SYSIBM.SYSFUNCTIONS
                WHERE SCHEMA = :schema_name
                AND NAME = :function_name;
        """)
        # print(query)
        with engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name, 'function_name': function_name})
            row = result.fetchone()
            if row:
                return {
                    function_name: {
                        "definition": row[1]
                    }
                }
            return {}
    except Exception as e:
        print(f"Error retrieving function schema: {e}")
        return {}


def get_stored_procedure_schema(engine, schema_name, proc_name):
    try:
        query = text("""
            SELECT PROCNAME, TEXT FROM SYSIBM.SYSPROCEDURES WHERE PROCSCHEMA = :schema_name
            AND PROCNAME = :proc_name;
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name, 'proc_name': proc_name})
            row = result.fetchone()
            if row:
                return {
                    proc_name: {
                        "definition": row[1]
                    }
                }
            return {}
    except Exception as e:
        print(f"Error retrieving stored procedure schema: {e}")
        return {}


def get_functions(engine, schema_name):
    try:
        query = text("""
            SELECT name FROM SYSIBM.SYSFUNCTIONS WHERE SCHEMA = :schema_name;
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name})
            functions = [row[0] for row in result]
            return functions
    except Exception as e:
        print(f"Error retrieving functions: {e}")
        return []


def get_stored_procedures(engine, schema_name):
    try:
        query = text("""
            SELECT PROCNAME FROM SYSIBM.SYSPROCEDURES WHERE PROCSCHEMA = :schema_name;
        """)
        with engine.connect() as conn:
            result = conn.execute(query, {'schema_name': schema_name})
            procedures = [row[0] for row in result]
            return procedures
    except Exception as e:
        print(f"Error retrieving stored procedures: {e}")
        return []


def get_schema(engine, schema_name, item_name, schema_type, TYPE):
    try:
        inspector = inspect(engine)
        if schema_type == 'tables':
            return get_table_schema(inspector, schema_name, item_name, TYPE)
        elif schema_type == 'views':
            return get_view_schema(inspector, schema_name, item_name, TYPE)
        elif schema_type == 'functions':
            return get_function_schema(engine, schema_name, item_name)
        elif schema_type == 'stored_procedures':
            return get_stored_procedure_schema(engine, schema_name, item_name)
        elif schema_type == 'triggers':
            return get_trigger_schema(engine, schema_name, item_name)
        else:
            raise ValueError(f"Invalid schema type: {schema_type}")
    except Exception as e:
        print(f"Error retrieving {schema_type} schema: {e}")
        return {}


def format_schema_for_json(schema):
    try:
        formatted_schema = {}
        for item_name, item_info in schema.items():
            if isinstance(item_info, dict):
                # Handle column definitions or other dictionary-based schema items
                if "definition" in item_info:
                    item_data = {"definition": item_info["definition"]}
                else:
                    item_data = {"datatype": item_info.get("datatype", "").split("(")[0].strip().lower()}
                    length_info = item_info.get("datatype", "").split("(")[1][:-1] if len(
                        item_info.get("datatype", "").split("(")) > 1 else None

                    if length_info:
                        if "," in length_info:
                            precision, scale = length_info.split(",")
                            item_data["precision"] = int(precision.strip())
                            item_data["scale"] = int(scale.strip())
                        else:
                            item_data["length"] = int(length_info.strip())

                    if "default" in item_info:
                        item_data["default"] = item_info["default"]

                    item_data["is_nullable"] = item_info.get("is_nullable", None)

            elif isinstance(item_info, list):
                # Handle lists, which are likely constraints or keys
                item_data = item_info

            else:
                # Handle unexpected types (optional, depending on your schema)
                item_data = str(item_info)

            formatted_schema[item_name] = item_data

        return formatted_schema
    except Exception as e:
        print(f"Error formatting {schema} schema: {e}")
        return {}


def save_schema_to_json(schema_data, output_file, schema_type):
    with open(output_file, 'w') as json_file:
        json.dump({schema_type: schema_data}, json_file, indent=4)
    print(f"{schema_type} schema saved to '{output_file}'.")


def read_lookup_file(lookup_file):
    with open(lookup_file, 'r') as file:
        items = [line.strip() for line in file if line.strip()]
    return items


def compare_schemas(source_schema, target_schema):
    differences = {}

    def compare_constraints(source_constraints, target_constraints):
        if source_constraints != target_constraints:
            return f"Mismatch: Source has {source_constraints} but Target has {target_constraints}"
        return None

    for item_name in source_schema:
        if item_name in target_schema:
            source_item_schema = source_schema[item_name]
            target_item_schema = target_schema[item_name]
            item_differences = []

            for column_name in source_item_schema:
                if column_name not in target_item_schema:
                    item_differences.append(f"Column '{column_name}' missing in target schema")
                elif source_item_schema[column_name] != target_item_schema[column_name]:
                    item_differences.append(
                        f"Column '{column_name}' mismatch: {source_item_schema[column_name]} "
                        f"!= {target_item_schema[column_name]}")

            # Compare constraints
            source_primary_key = source_item_schema.get('primary_key', [])
            target_primary_key = target_item_schema.get('primary_key', [])
            primary_key_diff = compare_constraints(source_primary_key, target_primary_key)
            if primary_key_diff:
                item_differences.append(primary_key_diff)

            source_foreign_keys = source_item_schema.get('foreign_keys', [])
            target_foreign_keys = target_item_schema.get('foreign_keys', [])
            foreign_key_diff = compare_constraints(source_foreign_keys, target_foreign_keys)
            if foreign_key_diff:
                item_differences.append(foreign_key_diff)

            source_unique_constraints = source_item_schema.get('unique_constraints', [])
            target_unique_constraints = target_item_schema.get('unique_constraints', [])
            unique_constraint_diff = compare_constraints(source_unique_constraints, target_unique_constraints)
            if unique_constraint_diff:
                item_differences.append(f"Unique constraints mismatch: {unique_constraint_diff}")

            source_check_constraints = source_item_schema.get('check_constraints', [])
            target_check_constraints = target_item_schema.get('check_constraints', [])
            check_constraint_diff = compare_constraints(source_check_constraints, target_check_constraints)
            if check_constraint_diff:
                item_differences.append(f"Check constraints mismatch: {check_constraint_diff}")

            # Compare indexes
            # source_indexes = source_item_schema.get('indexes', {})
            # target_indexes = target_item_schema.get('indexes', {})
            # if source_indexes != target_indexes:
            #     item_differences.append(f"Indexes mismatch: {source_indexes} != {target_indexes}")

            # Compare triggers
            # source_triggers = source_item_schema.get('triggers', {})
            # target_triggers = target_item_schema.get('triggers', {})
            # if source_triggers != target_triggers:
            #     item_differences.append(f"Triggers mismatch: {source_triggers} != {target_triggers}")

            if item_differences:
                differences[item_name] = item_differences
        else:
            differences[item_name] = ["Missing in target schema"]

    for item_name in target_schema:
        if item_name not in source_schema:
            differences[item_name] = ["Missing in source schema"]

    return differences


def generate_documentation(differences, output_dir, format):
    """
    Generate a summary report documenting the schema comparison results.
    :param differences: The differences in schemas as a dictionary.
    :param output_dir: The directory to save the report.
    :param format: The format of the documentation ('markdown' or 'html').
    """
    report = ""
    if format == "markdown":
        report += "# Schema Comparison Report\n\n"
        for item_name, diff_list in differences.items():
            report += f"## {item_name}\n"
            for diff in diff_list:
                report += f"- {diff}\n"
            report += "\n"
    elif format == "html":
        report += "<html><body>"
        report += "<h1>Schema Comparison Report</h1>"
        for item_name, diff_list in differences.items():
            report += f"<h2>{item_name}</h2>"
            report += "<ul>"
            for diff in diff_list:
                report += f"<li>{diff}</li>"
            report += "</ul>"
        report += "</body></html>"

    # Save the report to a file
    file_extension = "md" if format == "markdown" else "html"
    report_file = os.path.join(output_dir, f"SchemaComparisonReport.{file_extension}")
    with open(report_file, 'w') as file:
        file.write(report)
    print(f"Schema comparison report saved to '{report_file}'.")


def main():
    try:
        source_schema_name = config[source]['schema_name']
        target_schema_name = config[target]['schema_name']
        output_dir = config['output']['directory']
        comparison_types = config['COMPARISON']['compare'].split(",")  # Split comparison types by comma

        timestamp = datetime.now().strftime("SchemaValidator_%Y%m%d_%H%M%S")
        output_dir_with_timestamp = os.path.join(output_dir, timestamp)
        error_log_file = config['COMPARISON']['error_log_file']
        error_log_file_with_timestamp = os.path.join(output_dir, timestamp, error_log_file)

        os.makedirs(output_dir_with_timestamp, exist_ok=True)
        logging.basicConfig(filename=error_log_file_with_timestamp, level=logging.ERROR,
                            format='%(message)s')

        lookup_file = config['COMPARISON'].get('lookup_file', 'no').lower()

        # Define lookup files for each type
        lookup_files = {
            'tables': config['COMPARISON'].get('table_lookup_file', ''),
            'views': config['COMPARISON'].get('view_lookup_file', ''),
            'functions': config['COMPARISON'].get('function_lookup_file', ''),
            'stored_procedures': config['COMPARISON'].get('stored_procedure_lookup_file', '')
        }
        all_differences = {}

        for comparison_type in comparison_types:
            comparison_type = comparison_type.strip()
            print(f"Starting comparison for {comparison_type}...")  # Debugging statement

            # Create a subfolder based on comparison type
            output_dir_for_comparison = os.path.join(output_dir_with_timestamp, comparison_type)
            os.makedirs(output_dir_for_comparison, exist_ok=True)

            source_schema = {}
            target_schema = {}

            use_lookup_file = lookup_file != 'no'
            lookup_file_path = lookup_files.get(comparison_type, '')

            if use_lookup_file and os.path.exists(lookup_file_path):
                items_source = read_lookup_file(lookup_file_path)
                items_target = items_source
            else:
                if comparison_type == 'tables':
                    items_source = source_inspector.get_table_names(schema=source_schema_name)
                    items_target = target_inspector.get_table_names(schema=target_schema_name)
                elif comparison_type == 'views':
                    items_source = source_inspector.get_view_names(schema=source_schema_name)
                    items_target = target_inspector.get_view_names(schema=target_schema_name)
                elif comparison_type == 'functions':
                    items_source = get_functions(source_engine, source_schema_name)
                    items_target = get_functions(target_engine, target_schema_name)
                elif comparison_type == 'stored_procedures':
                    items_source = get_stored_procedures(source_engine, source_schema_name)
                    items_target = get_stored_procedures(target_engine, target_schema_name)
                else:
                    raise ValueError(f"Invalid comparison type specified: {comparison_type}")

            for item_name in items_source:
                print(f"Processing source {comparison_type[:-1]}: {item_name}")  # Debugging statement
                schema = get_schema(source_engine, source_schema_name, item_name, comparison_type, 'SOURCE')
                formatted_schema = format_schema_for_json(schema)
                source_schema[item_name] = formatted_schema

            for item_name in items_target:
                print(f"Processing target {comparison_type[:-1]}: {item_name}")  # Debugging statement
                schema = get_schema(target_engine, target_schema_name, item_name, comparison_type, 'TARGET')
                formatted_schema = format_schema_for_json(schema)
                target_schema[item_name] = formatted_schema

            source_output_file = os.path.join(output_dir_for_comparison, f'SourceSchema_{comparison_type}.json')
            target_output_file = os.path.join(output_dir_for_comparison, f'TargetSchema_{comparison_type}.json')

            save_schema_to_json(source_schema, source_output_file, f"SourceSchema_{comparison_type.capitalize()}")
            save_schema_to_json(target_schema, target_output_file, f"TargetSchema_{comparison_type.capitalize()}")

            differences = compare_schemas(source_schema, target_schema)
            differences_output_file = os.path.join(output_dir_for_comparison,
                                                   f'SchemaDifferences_{comparison_type}.json')
            save_schema_to_json(differences, differences_output_file, "SchemaDifferences")
            all_differences.update(differences)
            print(f"Completed comparison for {comparison_type}.\n")  # Debugging statement
        # Generate documentation
        generate_documentation(all_differences, output_dir_with_timestamp, 'markdown')
        generate_documentation(all_differences, output_dir_with_timestamp, 'html')
    except Exception as e:
        print(f"Unexpected error: {e}")
        exit(1)


if __name__ == "__main__":
    start_time = time.time()
    main()
    log_errors()
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken: {time_taken:.2f} seconds")
