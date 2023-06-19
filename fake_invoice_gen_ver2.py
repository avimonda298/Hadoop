# Last amended: 08th April, 2023
# Myfolder:  /home/ashok/Documents/fake
# Ref: https://www.macrometa.com/event-stream-processing/spark-structured-streaming

# Objective: Generate files with fake invoice data, one by one
#            Invoice files are generated at random intervals
#            and deposited in linux & hadoop directories
#	     Every generated file has a header.
# 	       cusid,item,price,qty,dateTime
#
#
#
"""
##******
## Usage
##******


# a. First create data folder in Linux:

rm -r -f /home/ashok/Documents/spark/data
mkdir -p /home/ashok/Documents/spark/data


# b. Generated files will also be saved/pushed to hadoop:

hdfs dfs -rm -r /user/ashok/spark/data
hdfs dfs -mkdir -p /user/ashok/spark/data

# c. Now start generating invoice files:

python /home/ashok/Documents/fake/fake_invoice_gen_ver2.py 

"""

"""
Sample csv data is:
cusid,item,price,qty,eventTime
1300,bourvita,50,52,13-04-2023 02:19
1001,horlicks,26,82,13-04-2023 09:32
9000,horlicks,16,95,13-04-2023 10:06
3200,curd,13,28,13-04-2023 10:17
1001,horlicks,15,140,14-04-2023 11:02
9000,dantmanjan,38,13,14-04-2023 11:43
8100,colgate,23,158,14-04-2023 12:06
6070,colgate,15,118,15-04-2023 12:42
1200,bourvita,35,33,15-04-2023 01:24

"""


# 1.0 Import needed libraries
import time
import datetime
import numpy as np
import random
import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


# 2.0 List of customer ids and productids:

customerId=[("1001"), ("2021"),("3200"),("4032"),("5200"),("6070"),("7090"),("8100"),("9000"),("1100"),("1200"),("1300" )]
productName = ["colgate","dantmanjan","horlicks","bourvita","boost","chilli","cardmom","curd","rice"] 


# 2.1 Current time
tnow = datetime.datetime.now()



# 3.0 Begin generating data

# 3.0.1 Some constants:

noOfFilesgenerated = 30   # files: invoice0.csv, invoice1.csv, invoice2.csv....
maxRecords         = 20   # Per file max records will be: 2 to maxRecords
recordGenInterval  = 2.0 # Every recordGenInterval secs a record is created
datafiledir        = "/home/ashok/Documents/spark/data/"
hadoopdir          = "/user/ashok/spark/data"

for i in range(noOfFilesgenerated):
    print("We begin our task...")
    filename = datafiledir + "invoice" + str(i) + ".csv"
    # Generate n rows of data
    n = random.randint(2,maxRecords)
    df = pd.DataFrame(columns = ['cusid','item','price','qty','dateTime'])
    for j in range(n):
        # Gen one record after some sleep
        time.sleep(recordGenInterval) 
        # 3.1 Time increments by this much after each transaction
        increment = random.randint(300, 3000)
        # 3.2 Add delta to time
        tnow = tnow+datetime.timedelta(0,increment)
        # 3.3 Convert time to string
        tnow_str = tnow.strftime("%d-%m-%Y %I:%M")
        # 3.4 Randomly select one of the customers,  products & quantity
        cus = random.choice(customerId)
        p_purchased = random.choice(productName)
        quantity = str(random.randint(10, 200))
        price = str(random.randint(10, 50))
        # 3.5 Process for output
        z = {'cusid' : cus, 'item': p_purchased,'price' : price, 'qty' : quantity, 'eventTime': tnow} 
        # 3.6 Append to dataframe
        df = df.append(z, ignore_index = True) 
        # 3.7 Save dataframe to a file    
        if (j == n-1 ):
            df.to_csv(filename, index = False)
            print(f"Saved file: {filename}") 
            # Push this file to hadoop
            cmd = "hdfs dfs -put " + filename + " " + hadoopdir
            os.system(cmd)

            


############# Done ###############
