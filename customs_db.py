import sqlite3

# Customs Database Metadata:
sqlite_file = "customs_dg.sqlite"

arrivals_table = {'name': 'arrivals',
                  'fields': ['id',
                  			 'arrival_time',
                  			 'passengers_dom',
                  			 'passengers_intl'],
                  'types': ['INTEGER',
                  			'TEXT',
                  			'INTEGER',
                  			'INTEGER']
                 }

servers_table = {'name': 'servers',
				 'fields': ['id',
				 			'subsection',
				 			'0000-0400',
				 			'0400-0800',
				 			'0800-1200',
				 			'1200-1600',
				 			'1600-2000',
				 			'2000-2400'],
				'types': ['INTEGER',
				 		  'TEXT',
				 		  'INTEGER',
				 		  'INTEGER',
				 		  'INTEGER',
				 		  'INTEGER',
				 		  'INTEGER',
				 		  'INTEGER']
				}

# Create Database by opening a connection for the first time.
connection = sqlite3.connect(sqlit_file)
cursor = connection.cursor()

# Create new SQLite tables.
arrivals_table_create_query = ('CREATE TABLE arrivals ('
								 'id integer PRIMARY KEY, '
								 'arrival_time text, '
								 'passengers_dom integer, '
								 'passengers_intl integer, '
								 'table_constraint) '
								 '[WITHOUT ROWID];')

servers_table_create_query = ('CREATE TABLE servers ('
							    'id integer PRIMARY KEY, '
								'subsection text, '
								'\'0-4\' integer, '
								'\'4-8\' integer, '
								'\'8-12\' integer, '
								'\'12-16\' integer, '
								'\'16-20\' integer, '
								'\'20-24\' integer, '
								'table_constraint) '
								 '[WITHOUT ROWID];')

cursor.execute(arrivals_table_create_query)
cursor.execute(servers_table_create_query)

# Commit changes and close the connection to the database file.
connection.commit()
connection.close()


