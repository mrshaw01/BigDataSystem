import happybase

connection = happybase.Connection(host='node-master', port=9090)
tables = connection.tables()
print("Tables: ", tables)

table = connection.table('categories_table')

count = 0
for row in table.scan():
	count += 1
	print(f"row={count}, key={row[0]}, columns={row[1]}")

