import happybase

# establish a connection to HBase
connection = happybase.Connection("node-master", 9090)

# table name and column families
table_name = "products_table"
column_families = {"product_info": dict()}

# drop if the table exists
print(connection.tables())
if table_name.encode() in connection.tables():
	print("Drop existing table")
	connection.delete_table(table_name, disable=True)

# create new table
print("Create new table")
connection.create_table(table_name, column_families)

