
import pymysql
import csv
import boto3
import configparser


# load mysql connection info ---------------------
parser = configparser.ConfigParser()  
parser.read('pipeline.conf')

hostname = parser.get('mysql_config','hostname')
dbname = parser.get('mysql_config','database')
port = parser.get('mysql_config','port')
username = parser.get('mysql_config','username')
password = parser.get('mysql_config','password')


# establish a connection -------------------------
conn = pymysql.connect(host=hostname,
	user=username,
	password=password,
	db=dbname,
	port=int(port))

if conn is None:
	print('Error connecting to the db')
else:
	print('MySQL connection established')


# pull data and write to local csv ---------------
m_sql = 'select * from orders;'
local_file = 'orders_extract.csv'

m_cursor = conn.cursor()
m_cursor.execute(m_sql)
results = m_cursor.fetchall()

with open(local_file, 'w') as fp:
	csv_w = csv.writer(fp,delimiter=',')
	csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()


# aws boto credentials upload csv - ---------------

parser = configparser.ConfigParser()
parser.read('pipeline.conf')

access_key = parser.get('aws_boto_credentials','access_key')
secret_key = parser.get('aws_boto_credentials','secret_key')
bucketname = parser.get('aws_boto_credentials','bucket_name')

s3_client = boto3.client('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#s3_file = local_file
s3_client.upload_file(local_file, bucketname, 'pockets/' + local_file)

# alt ---

s3_resource = boto3.resource('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
s3_resource.meta.client.upload_file(local_file, bucketname, 'pockets/'+'line_69.csv') #s3_file)

#response = s3_client.list_buckets()
#for b in response['Buckets']:
#    print(b['Name'])

# s3 = boto3.client('s3')
# with open(local_file, "rb") as f:
#    s3.upload_fileobj(f, bucketname, s3_file)



