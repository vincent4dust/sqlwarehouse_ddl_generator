# sqlwarehouse_ddl_generator
Assumption:
1. The view schema name is concat from the database schema name
2. Only string value in column db_name, tbl_name, col_name
3. Make sure the source input file is csv format & no redundant row at the last row

How to run the program?
1. Fill in ../config/parameter_config.properties
- SOURCE_FILE_NAME
- ENV
2. Fill in ../config/table_list.txt
3. Put the source file (csv format) in ../source/
4. Run ../app/main.py