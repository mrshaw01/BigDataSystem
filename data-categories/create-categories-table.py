import happybase

# establish a connection to HBase
connection = happybase.Connection("node-master", 9090)

# create a table
table_name = "categories_table"
column_families = {"category_info": dict()}
connection.create_table(table_name, column_families)

