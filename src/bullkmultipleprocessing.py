import psycopg2
from multiprocessing import Pool
import json

with open('config.json') as file:
    config = json.load(file)


host = config['host']
database = config['database']
user = config['user']
password = config['password']
port = config['port']
ssl_mode = config['ssl_mode']

def load_data(file):
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    with open(file, "r") as f:
        print("Started Copy From")
        cursor.copy_from(f, "author_bis", sep=";", null='null')
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    files = ["/Users/youssef/Desktop/Bookshelved/src/outputcsv/output_1.csv", 
             "/Users/youssef/Desktop/Bookshelved/src/outputcsv/output_2.csv", 
             "/Users/youssef/Desktop/Bookshelved/src/outputcsv/output_3.csv", 
             "/Users/youssef/Desktop/Bookshelved/src/outputcsv/output_4.csv"]
    print("Launching pools")
    pool = Pool(processes=len(files))
    pool.map(load_data, files)
    pool.close()
    pool.join()
