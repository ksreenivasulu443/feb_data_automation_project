import os


batch_date = os.getenv('BATCH_DATE')
table = os.getenv('TABLE')

# Print batch_date and table
print("Batch Date:", batch_date)
print("Table Name:", table)

print("All Spark properties:")
for prop in sc.getConf().getAll():
    print(prop)




