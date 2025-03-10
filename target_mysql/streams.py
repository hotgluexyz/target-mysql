"""Stream class for target-mssql."""
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from .singer_sdk.stream import Stream
from datetime import datetime
import json
import dateutil.parser
import logging
import pyodbc
import math
import base64
from decimal import Decimal
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

#TODO: Use logging 
#TODO: Opening Database conneciton on a per stream basis seems like a no-go
class MSSQLStream(Stream):
  
    """Stream class for MSSQL streams."""
    def __init__(self, conn, schema_name, batch_size, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = conn
        self.conn.autocommit=True
        #TODO Think about the right way to handle this when restructuring classes re https://pymssql.readthedocs.io/en/stable/pymssql_examples.html#important-note-about-cursors
        self.cursor = self.conn.cursor()
        self.dml_sql = None
        self.properties_dict = {}
        self.batch_cache = []
        self.batch_size = 1000 if batch_size is None else batch_size
        self.full_table_name = self.generate_full_table_name(self.name, schema_name)
        self.temp_full_table_name = self.generate_full_table_name(f"{self.name}_temp", schema_name)
        self.table_handler()

    def generate_full_table_name(self, streamname, schema_name):
        table_name = streamname
        table_name = streamname.replace("-","_")
        table_name = f"`{table_name}`"
        if schema_name: table_name = f"`{schema_name}`" + "." + table_name
        return table_name

    #TODO this method seems needless, should probably just call methods directly
    def table_handler(self):
        #TODO it is not safe to assume you can truncate a table in this situation
        
        #TODO How do we know all of the data is through and we are ready to drop and merge data into the table?
        
        ddl = self.schema_to_temp_table_ddl(self.schema, self.temp_full_table_name)
        self.ddl = ddl #Need access to column types when doing data processing( ie VARBINARY b64 decode)
        self.sql_runner(ddl)
  
    def schema_to_temp_table_ddl(self, schema, table_name) -> str:
        self.name_type_mapping = {} #Created dict for Name to Value mapping, ultimeately for data conversion. TODO this is a bit clunky
        primary_key=None
        try:
            if(self.key_properties[0]):
                primary_key = self.key_properties[0]
        except AttributeError:
            primary_key=None

        properties=self.schema["properties"]

        #TODO better system for detecting tables

        #TODO Need be using named parameters for SQL to avoid potential injection, and to be clean
        sql = f"CREATE TABLE {table_name}("

        #Key Properties
        #TODO can you assume only 1 primary key?
        if (primary_key):
            pk_type=self.ddl_json_to_mssqlmapping(self.schema["properties"][primary_key], pk=True)
            #TODO Can't assume this is an INT always
            #TODO 450 is silly
            pk_type=pk_type.replace("MAX","450") #TODO hacky hacky
            pk_type = "VARCHAR(450)" if pk_type == "TEXT" else pk_type
            sql += f"`{primary_key}` {pk_type} NOT NULL PRIMARY KEY,"
            properties.pop(primary_key, None) #Don't add the primary key to our DDL again

    
        #Loop through all properties of stream to add them to our DDL
        first=True
        for name, shape in properties.items():
            name = name.strip()
            mssqltype=self.ddl_json_to_mssqlmapping(shape)
            self.properties_dict[name]= None
            if (mssqltype is None): continue #Empty Schemas
            mssqltype=self.ddl_json_to_mssqlmapping(shape)
            self.name_type_mapping.update({name:mssqltype}) #TODO clunky for data conversation
            if(first):
                sql+= f" `{name}` {mssqltype}"
                first=False
            else:
                sql+= f", `{name}` {mssqltype}"


        sql += ") ENGINE=InnoDB ROW_FORMAT=COMPRESSED ;"
        return sql

    #TODO what happens with multiple types
    def ddl_json_to_mssqlmapping(self, shape:dict, pk=False) -> str:
        #TODO need to prioritize which type first
        if ("type" not in shape): return None
        jsontype = shape["type"]
        json_description = shape.get("description", None)
        json_max_length = shape.get("maxLength",None)
        json_format = shape.get("format", None)
        json_minimum = shape.get("minimum", None)
        json_maximum = shape.get("maximum", None)
        json_exclusive_minimum = shape.get("exclusiveMinimum", None)
        json_exclusive_maximum = shape.get("exclusiveMaximum", None)
        json_multiple_of = shape.get("multipleOf", None)
        mssqltype : str = None
        if ("string" in jsontype):
            if(json_max_length and json_max_length < 8000 and json_description != "blob"):
                mssqltype = f"VARCHAR({json_max_length})"
            elif(json_description == "blob"):
                mssqltype = f"VARBINARY(255)"
            elif(json_format == "date-time" and json_description == "date"):
                mssqltype = f"Date"
            elif(json_format == "date-time"):
                mssqltype = f"Datetime"
            elif pk:
                mssqltype = "TEXT"
            else:
                mssqltype = "LONGTEXT"
        elif ("number" in jsontype):
            mssqltype = "BIGINT"
        elif ("number2" in jsontype):
            if (json_minimum and json_maximum and json_exclusive_minimum and json_exclusive_maximum and json_multiple_of):
                #https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver15
                #p (Precision) Total number of decimal digits
                #s (Scale) Total number of decimal digits to the right of the decimal place

                max_digits_left_of_decimal = math.log10(json_maximum)
                max_digits_right_of_decimal = -1*math.log10(json_multiple_of)
                #percision : int = int(max_digits_left_of_decimal + max_digits_right_of_decimal)
                #scale : int = int(max_digits_right_of_decimal)
                percision : int = int(10)
                scale : int = int(10)
                mssqltype = f"NUMERIC({percision},{scale})"
            else:
                mssqltype = "NUMERIC(19,6)"
        elif ("integer" in jsontype):
            mssqltype = "INT"
        elif ("boolean" in jsontype):
            mssqltype = "BIT"

        elif ("array" in jsontype or "object" in jsontype):
            mssqltype = "VARCHAR(1023)"
        #not tested
        elif ("null" in jsontype):
            raise NotImplementedError("Can't set columns as null in MYSQL")
        else:
            raise NotImplementedError(f"Haven't implemented dealing with this type of data. jsontype: {jsontype}")

        return mssqltype

    def convert_data_to_params(self, datalist) -> list:
        parameters = []
        for noop in datalist:
            parameters.append("?")
        return parameters
     
    #TODO when this is batched how do you make sure the column ordering stays the same (data class probs)
    #Columns is seperate due to data not necessairly having all of the correct columns
    def record_to_dml(self, table_name:str, data:dict) -> str:
        #TODO this is a bit gross, could refactor to make this easier to read
        #replace empty last spaces
        data = {x.strip(): v for x, v in data.items()}
        column_list="`,`".join(data.keys())
        sql = f"INSERT INTO {table_name} (`{column_list}`)"
        paramaters = self.convert_data_to_params(data.values())
        sqlparameters = ",".join(paramaters)
        sql += f" VALUES ({sqlparameters})"
        return sql

    def sql_runner(self, sql):
        try:
            logging.info(f"Running SQL: {sql}")
            self.cursor.execute(sql)
        except pyodbc.ProgrammingError as e:
            if "already exists" in str(e):
                logging.info(f"Existing table found, dropping table: {self.temp_full_table_name}")
                drop_sql = f"DROP TABLE IF EXISTS {self.temp_full_table_name}"
                self.cursor.execute(drop_sql)
                logging.info(f"Re-running SQL: {sql}")
                self.cursor.execute(sql)
        except Exception as e:
            logging.error(f"Caught exception whie running sql: {sql}")
            raise e

    def sql_runner_withparams(self, sql, parameters):
        self.batch_cache.append(parameters)
        if(len(self.batch_cache)>=self.batch_size):
            logging.info(f"Running batch with SQL: {sql} . Batch size: {len(self.batch_cache)}")
            self.commit_batched_data(sql, self.batch_cache)
            self.batch_cache = [] #Get our cache ready for more!

    def commit_batched_data(self, dml, cache):
        try:
            self.conn.autocommit = False
            self.cursor.fast_executemany = False
            logging.info(dml)
            logging.info(cache[0])
            self.cursor.executemany(dml, cache)
        except pyodbc.DatabaseError as e:
            # logging.error(f"Caught exception while running batch sql: {dml}. ")
            logging.error(f"Caught exception while running batch sql: {dml}. Parameters for batch: {cache[0]} ")
            self.conn.rollback()
            raise e
        except pyodbc.Error as e:
            if e.args[0] == 'HY000':
                logging.error(f"Caught exception while running batch sql: {dml}. Parameters for batch: {cache[0]} ")
                logging.info("Rolling back transaction")
                self.conn.rollback()
                raise e
        else:
            self.conn.commit()
        finally:
            self.cursor.fast_executemany = False
            self.conn.autocommit = True #Set us back to the default of autoCommiting for other actions

    def data_conversion(self, name_ddltype_mapping, record):
        newrecord = record
        if ("VARBINARY(max)" in name_ddltype_mapping.values() or
                "Date" in name_ddltype_mapping.values() or
                "Datetime2(7)" in name_ddltype_mapping.values() or
                "Datetime" in name_ddltype_mapping.values()  or
                "INT" in name_ddltype_mapping.values() or
                "VARCHAR(255)" in name_ddltype_mapping.values()
        ):
            for name, ddl in name_ddltype_mapping.items():
                if ddl=="VARBINARY(max)":
                    b64decode = None
                    if (record.get(name) is not None): b64decode = base64.b64decode(record.get(name))
                    #Tested this with the data that lands in the MSSQL database
                    #Take the hex data and convert them to bytes
                    #bytes = bytes.fromhex(hex) #remove hex indicator 0x from hex
                    #with open('file2.png', 'wb') as file
                    #  file.write(bytes)
                    #Example I used was a png, you'll need to determine type
                    record.update({name:b64decode})
                #https://gitlab.com/meltano/sdk/-/blob/main/singer_sdk/helpers/_typing.py#L179 looks to be a much better implementation, https://gitlab.com/autoidm/autoidm-target-mssql/-/issues/39 is in to migrate.
                elif ddl=="Date":
                    date = record.get(name)
                    if (date is not None): 
                        transformed_date = dateutil.parser.isoparse(date)
                        newdate = transformed_date.strftime("%Y-%m-%d")
                        record.update({name:newdate})
                elif ddl=="Datetime2(7)" or ddl=="Datetime":
                    date = record.get(name)
                    if (date is not None):
                        transformed_date = dateutil.parser.isoparse(date)
                        newdate = transformed_date.strftime("%Y-%m-%d %H:%M:%S.%f")
                        record.update({name:newdate})
                elif ddl=="INT" or ddl=="BIGINT":
                    val = record.get(name)
                    if (val is not None):
                        record.update({name:int(val)})
                elif ddl == "VARCHAR(255)":
                    val = record.get(name)
                    if (val is not None):
                        record.update({name: val[:255]})
                elif ddl == "VARCHAR(1023)":
                    val = record.get(name)
                    if (val is not None):
                        record.update({name: json.dumps(val, default=str)[:1023]})
                elif ddl=="JSON":
                    val = record.get(name)
                    if (val is not None):
                        record.update({name:json.dumps(val, default=str)})

        return newrecord

    #Not actually persisting the record yet, batching
    def persist_record(self, record):
        for prop in record:
            self.properties_dict[prop]= record[prop]
        dml= self.record_to_dml(table_name=self.temp_full_table_name, data=self.properties_dict)
        self.dml_sql = dml
        record = self.properties_dict
        record = self.data_conversion(self.name_type_mapping, record)
        self.sql_runner_withparams(dml, tuple(record.values()))

    def clean_up(self):
        #Commit any batched records that are left
        if(len(self.batch_cache)>0):
            logging.info(f"Running batch with SQL: {self.dml_sql} . Batch size: {len(self.batch_cache)}")
            self.commit_batched_data(self.dml_sql, self.batch_cache)
        #We are good to go, drop table if it exists
        sql = f"CREATE TABLE IF NOT EXISTS {self.full_table_name} LIKE {self.temp_full_table_name}"
        self.sql_runner(sql)
        #Rename our temp table to the correct table
        sql = f"REPLACE INTO {self.full_table_name} SELECT * FROM {self.temp_full_table_name}"
        self.sql_runner(sql)
        #Remove temp table
        sql = f"DROP TABLE IF EXISTS {self.temp_full_table_name}"
        self.sql_runner(sql)
