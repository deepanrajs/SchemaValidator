Schema Validator Tool
Overview
The Schema Validator Tool is designed to compare database schemas across different systems and validate them against expected structures. It supports Oracle and DB2 databases and provides detailed comparison results including mismatches and missing records.

Features
Compare schemas for tables, views, functions, and stored procedures.
Support for Oracle and DB2 databases.
Output results to a timestamped directory with detailed logs.
Configuration
The tool uses a configuration file to define connection settings, comparison options, and file paths. The configuration file should be structured as follows:

Comparison Section
ini
Copy code
[COMPARISON]
SOURCE = SYSTEM
TARGET = APPQOSSYS
# Options: 'tables', 'views', 'functions', 'stored_procedures'
compare = tables
SOURCE: The source schema to compare.
TARGET: The target schema to compare against.
compare: Specifies which schema objects to compare (e.g., tables, views, functions, stored_procedures).
Queries Section
ini
Copy code
[QUERIES]
FUNCTIONS_LIST = SELECT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = :schema_name AND OBJECT_TYPE = 'FUNCTION'
FUNCTIONS_SCHEMA = SELECT OBJECT_NAME, DBMS_METADATA.GET_DDL(OBJECT_TYPE, OBJECT_NAME, OWNER) AS DDL FROM ALL_OBJECTS WHERE OWNER = :schema_name AND OBJECT_NAME = :function_name
STORED_PROCEDURE_LIST = SELECT OBJECT_NAME FROM ALL_OBJECTS WHERE OWNER = :schema_name and OBJECT_TYPE = 'PROCEDURE'
STORED_PROCEDURE_SCHEMA = SELECT OBJECT_NAME, DBMS_METADATA.GET_DDL(OBJECT_TYPE, OBJECT_NAME, OWNER) AS DDL FROM ALL_OBJECTS WHERE OWNER = :schema_name AND OBJECT_NAME = :proc_name
FUNCTIONS_LIST: SQL query to retrieve the list of functions.
FUNCTIONS_SCHEMA: SQL query to retrieve the DDL of a specific function.
STORED_PROCEDURE_LIST: SQL query to retrieve the list of stored procedures.
STORED_PROCEDURE_SCHEMA: SQL query to retrieve the DDL of a specific stored procedure.
Lookup Files Section
ini
Copy code
[LOOKUP_FILES]
lookup_file = yes
lookup_folder = lookup_files
error_log_file = error_log.txt
terminal_log_file = log.txt
table_lookup_file = lookup_tables.txt
view_lookup_file = lookup_view.txt
function_lookup_file = lookup_functions.txt
stored_procedure_lookup_file = lookup_sp.txt
lookup_file: Indicates whether to use lookup files (yes or no).
lookup_folder: Folder containing lookup files.
error_log_file: Path to the error log file.
terminal_log_file: Path to the terminal log file.
table_lookup_file, view_lookup_file, function_lookup_file, stored_procedure_lookup_file: Paths to the respective lookup files.
System and Target Database Connections
ini
Copy code
[SYSTEM]
driver = oracle+cx_oracle
username = SYSTEM
password = Temp4now
host = localhost
port = 1521
sid = orcl
service_name = orcl.corp.mastechinfotrellis.com
database = orcl.corp.mastechinfotrellis.com
schema_name = SYSTEM

[APPQOSSYS]
driver = oracle+cx_oracle
username = APPQOSSYS
password = Temp4now
host = localhost
port = 1521
sid = orcl
service_name = orcl.corp.mastechinfotrellis.com
database = orcl.corp.mastechinfotrellis.com
schema_name = APPQOSSYS

[DB2_1]
driver = ibm_db_sa
username = db2admin
password = db2@dmin
host = 10.100.15.32
port = 50000
database = MDMQADB
schema_name = DB2ADMIN

[DB2_2]
driver = ibm_db_sa
username = mdmadmin
password = mdm@dmin123
host = 10.100.15.32
port = 50000
database = MDMQADB
schema_name = MDMADMIN
[SYSTEM], [APPQOSSYS], [DB2_1], [DB2_2]: Database connection configurations for Oracle and DB2 systems.
driver: Database driver to use.
username, password: Credentials for connecting to the database.
host, port, sid, service_name, database: Connection details.
schema_name: The schema name to connect to.
Output Configuration
ini
Copy code
[output]
directory = C:\\Users\\MITDeepanraj\\PycharmProjects\\schemaValidator\\output
directory: Path to the output directory where results will be saved.
Usage
Update the configuration file (config.ini) with your database details and desired settings.
Run the tool using your preferred method (e.g., command line or IDE).
Check the output and log files for results and error information.
License
This tool is provided as-is without any warranty. Use it at your own risk.

Contact
For support or questions, please contact Gowthambaalaji Sekhar at gowtham.s@mastechdigital.com.
