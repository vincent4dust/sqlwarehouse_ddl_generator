import os, sys
import pandas as pd
from datetime import datetime
import configparser
import logging

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARAMETER_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "parameter_config.properties")
SOURCE_FILE_FOLDER = os.path.join(PROJECT_ROOT, "source")
TABLE_LIST_PATH = os.path.join(PROJECT_ROOT, "config", "table_list.txt")
OUTPUT_FILE_FOLDER = os.path.join(PROJECT_ROOT, "output")
LOG_FILE_PATH = os.path.join(PROJECT_ROOT, "log", "log.txt")

logging.basicConfig(filename=LOG_FILE_PATH, level=logging.INFO, format="%(asctime)s :: %(levelname)s :: %(message)s")

def get_parameter_config(parameter_config_path=PARAMETER_CONFIG_PATH):
    config_parser = configparser.ConfigParser()
    config_parser.read(parameter_config_path)
    config = {}
    config["DEFAULT"] = {}

    if len(config_parser.sections()) != 0:
        for sec in config_parser.sections():
            config[sec] = {}
    
    for sec in config:
        for key in config_parser[sec]:
            config[sec][key.upper()] = config_parser[sec][key]

    return config

def get_table_list(table_list_path=TABLE_LIST_PATH):
    table_list = []
    if os.path.exists(table_list_path):
        with open(table_list_path, "r") as in_file:
            for line in in_file:
                table_list.append(line.rstrip().upper())
    else:
        logging.error("'{}' not exists".format(table_list_path))
        raise UserWarning("{} :: ERROR :: '{}' not exists".format(datetime.now(), table_list_path))
    
    return table_list

def read_input_file(source_file, source_folder=SOURCE_FILE_FOLDER):
    csv_source_file = os.path.join(source_folder, source_file)
    if os.path.exists(csv_source_file):
        if csv_source_file.endswith(".csv"):
            src_data = pd.read_csv(csv_source_file)
            src_data.columns = map(str.upper, src_data.columns)
    else:
        logging.error("'{}' not exists".format(csv_source_file))
        raise UserWarning("{} :: ERROR :: '{}' not exists".format(csv_source_file))

    return src_data

def get_sql(src_data, table_list, ad_group):
    src_col = src_data.columns
    if "DB_NAME" in src_col:
        src_data["DB_NAME"] = src_data["DB_NAME"].apply(lambda x: x.upper())
    else:
        logging.error("No column 'DB_NAME' found in source file")
        raise UserWarning("{} :: ERROR :: No column 'DB_NAME' found in source file".format(datetime.now()))

    if "TBL_NAME" in src_col:
        src_data["TBL_NAME"] = src_data["TBL_NAME"].apply(lambda x: x.upper())
    else:
        logging.error("No column 'TBL_NAME' found in source file")
        raise UserWarning("{} :: ERROR :: No column 'TBL_NAME' found in source file".format(datetime.now()))

    if "COL_NAME" in src_col:
        src_data["COL_NAME"] = src_data["COL_NAME"].apply(lambda x: x.upper())
    else:
        logging.error("No column 'COL_NAME' found in source file")
        raise UserWarning("{} :: ERROR :: No column 'COL_NAME' found in source file".format(datetime.now()))
    
    sql_stmt = ""
    for table_name in table_list:
        try:
            temp_df = src_data[src_data["TBL_NAME"] == table_name]

            db_name_list = temp_df["DB_NAME"].unique()
            if len(db_name_list) == 1:
                db_schema_name = db_name_list[0]
            else:
                logging.error("'DB_NAME' is not unique for table '{}'".format(table_name))
                raise UserWarning("{} :: ERROR :: 'DB_NAME' is not unique for table '{}'".format(datetime.now(), table_name))

            view_schema_name = db_schema_name[:-6]

            col_list = temp_df["COL_NAME"].tolist()
            col_sql_stmt = ""
            for col_name in col_list:
                conf_value = temp_df[temp_df["COL_NAME"] == col_name]["DATA CLASSIFICATION"].values.astype(str)[0]
                if conf_value == "Highly Confidential":
                    col_sql_stmt += f"CASE\n\tWHEN IS_MEMBER('{ad_group}') THEN {col_name}\n\tELSE 'NO ACCESS'\nEND AS {col_name},\n"
                else:
                    col_sql_stmt += f"{col_name},\n"

            his_table_name = table_name + "_HIS"
            col_his_sql_stmt = col_sql_stmt
            his_col_list = ["IS_ACTIVE", "EFF_ST_DT", "EFF_END_DT"]
            for his_col in his_col_list:
                col_his_sql_stmt += f"{his_col},\n"

            col_sql_stmt = col_sql_stmt.rstrip()
            col_sql_stmt = col_sql_stmt[:-1]
            col_his_sql_stmt = col_his_sql_stmt.rstrip()
            col_his_sql_stmt = col_his_sql_stmt[:-1]

            create_sql_stmt = f"CREATE OR REPLACE VIEW {view_schema_name}.{table_name}\nAS\nSELECT\n{col_sql_stmt}\nFROM\n{db_schema_name}.{table_name};"
            owner_sql_stmt = f"ALTER VIEW {view_schema_name}.{table_name} OWNER TO HIVE_OWNER;"

            create_his_sql_stmt = f"CREATE OR REPLACE VIEW {view_schema_name}.{his_table_name}\nAS\nSELECT\n{col_his_sql_stmt}\nFROM\n{db_schema_name}.{his_table_name};"
            owner_his_sql_stmt = f"ALTER VIEW {view_schema_name}.{his_table_name} OWNER TO HIVE_OWNER;"

            sql_stmt += create_sql_stmt
            sql_stmt += "\n"
            sql_stmt += owner_sql_stmt
            sql_stmt += "\n\n"

            print("{} :: INFO :: Done for {}".format(datetime.now(), table_name))
            logging.info("Done for {}".format(table_name))

            sql_stmt += create_his_sql_stmt
            sql_stmt += "\n"
            sql_stmt += owner_his_sql_stmt
            sql_stmt += "\n\n"

            print("{} :: INFO :: Done for {}".format(datetime.now(), his_table_name))
            logging.info("Done for {}".format(his_table_name))

        except Exception as e:
            logging.error(e)
            raise UserWarning("{} :: ERROR :: {}".format(datetime.now(), e))

    return sql_stmt

def write_to_output(curr_time, sql_stmt, output_folder=OUTPUT_FILE_FOLDER):
    file_name = f"{curr_time}.sql"
    output_file_name = os.path.join(output_folder, file_name)
    try:
        with open(output_file_name, "w") as out_file:
            out_file.write(sql_stmt)
    except Exception as e:
        logging.error(e)
        raise UserWarning("{} :: ERROR :: Failed to write to output folder".format(datetime.now()))

def main():
    curr_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.info("########################### {}.sql ###########################".format(curr_time))

    config = get_parameter_config()
    table_list = get_table_list()
    source_file = config["DEFAULT"]["SOURCE_FILE_NAME"]
    logging.info("Source file name: {}".format(source_file))
    ad_group = config["DEFAULT"]["MEMBER_AD_GROUP"]
    logging.info("AD group: {}".format(ad_group))
    src_data = read_input_file(source_file)
    sql_stmt = get_sql(src_data, table_list, ad_group)
    write_to_output(curr_time, sql_stmt, output_folder=OUTPUT_FILE_FOLDER)

if __name__ == "__main__":
    main()