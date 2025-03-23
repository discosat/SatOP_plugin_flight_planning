import psycopg2

class StorageDatabase:
    def __init__(self):
        self.connection = self.get_connection()
		
    def get_connection(self):
        try:
            return psycopg2.connect(
                database="disco_fp_db",
                user="postgres",
                password="password",
                host=""
            )
        except:
            return False
        
    def close_connection(self):
        self.connection.close()
        return True
    
    # def create_table(self, table_name, columns):
          



# def get_connection():
# 	try:
# 		return psycopg2.connect(
# 			database="postgres",
# 			user="postgres",
# 			password="password",
# 			host="127.0.0.1",
# 			port=5432,
# 		)
# 	except:
# 		return False
# conn = get_connection()
# if conn:
# 	print("Connection to the PostgreSQL established successfully.")
# else:
# 	print("Connection to the PostgreSQL encountered and error.")
