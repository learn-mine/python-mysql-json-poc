'''
    Requirements:
    
    0. Import the csv data into the database
    1. Task is to use a spark dataframe to group attached data using below query as-is
    2. Once the query result is in dataframe, then partititon the data by geography, ethnicity
    3. Create a json file with naming convention geography-ethinicity values. for instance, all-england-Asian.json
    4. File path should be /geography/ethnicity/geography-ethnicity.json
    
    SELECT * FROM gcse_datastore.gcse_eng_maths;
    
    SELECT geography, ethnicity, gender, SUM(denominator) AS TotalValue
    FROM gcse_datastore.gcse_eng_maths
    WHERE ethnicity <> 'All' AND geography <> 'All - England'
    GROUP BY geography, ethnicity, gender
    ORDER BY 1,2,3;
    
    geography				ethnicity	JSON Content required
    Barking and Dagenham	Asian		[
                                           {
                                              "All":555,
                                              "Boys":270,
                                              "Girls":285
                                           }
                                        ]
    Barking and Dagenham	Black	[
                                       {
                                          "All":733,
                                          "Boys":399,
                                          "Girls":334
                                       }
                                    ]
                                
    Pre-requisite:
        Python 3.12.0
        Mysql 8.0.27
        Mysql Workbench
        Environmental path variable
        
    Python libraries:
        pip install pandas
        pip install mysql.connector
  
    mysql:
        Log-in-as-windows administrator
        mysqld; <kick start the daemon>
        mysql -u root -p password; <username and password>
        CREATE SCHEMA `gcse_datastore`; <create a schema>
        
    Run: 
        python db_load_script.py
'''

# import libraries
import datetime
import errno
import os
import mysql.connector
from mysql.connector import errorcode
import pandas as pd

# Step: Import the CSV File into a dataFrame
#gcseData = pd.read_csv (r'C:\Users\44784\Desktop\wrk\python\gcse-english-and-maths-results-local-authority-data-for-2019-to-2020-2.csv')   
gcseData = pd.read_csv (r'C:\Users\44784\Desktop\wrk\python\gcse-test.csv')   
dataFrame = pd.DataFrame(gcseData)

# Step: Display the dataFrame
def displayData():
    print("GCSE Data")
    print(dataFrame)
    
# Step: Connect Python to mySql Database server
def connectToSql():
    try:
        conn = mysql.connector.connect(host='localhost', database='gcse_datastore', user='root', password='password', auth_plugin='mysql_native_password')
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("select database();")
            db = cursor.fetchone()[0]
            print("Connection Made To Database: ", db)
            return conn, cursor
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    else:
      cnx.close()            

# Step: Disconnect the database connection
def disconnectToSql(conn, cursor):
    if (conn.is_connected()):
        cursor.close()
        conn.close()
        print("MySQL connection is closed", "\n")    
        
# Step: Load csv dataframe to database
def loadCsvToDatabase():
    try:
        conn, cursor = connectToSql()
        #cursor = connectToSql()
        
        """ SCHEMA STRUCTURE: Sample
            Measure	Percentage achieving 9-5 in English & mathematics for children aged 14 to 16 (key stage 4)
            Ethnicity	Asian
            Ethnicity_type	ONS 2011 5+1
            Time	2019-2020
            Time_type	Academic year
            Geography	Derby
            Geography_type	Local authority
            Geography_code	E06000015
            Gender	Boys
            Value	43.9
            Value_type	%
            Denominator	285
            Numerator	125
        """

        # Create a new Table
        cursor.execute('DROP TABLE IF EXISTS gcse_eng_maths;')
        cursor.execute('''
            CREATE TABLE gcse_eng_maths (
                ID INT NOT NULL AUTO_INCREMENT,
                Measure VARCHAR(255) NOT NULL,
                Ethnicity VARCHAR(8) NOT NULL, 
                Ethnicity_type VARCHAR(16) NOT NULL, 
                Time VARCHAR(16) NOT NULL,
                Time_type VARCHAR(16) NOT NULL,
                Geography VARCHAR(255) NOT NULL,
                Geography_type VARCHAR(16) NOT NULL,
                Geography_code VARCHAR(16) NOT NULL,
                Gender VARCHAR(8) NOT NULL,
                Value DECIMAL(4,1) NOT NULL,
                Value_type CHAR(1) NOT NULL,
                Denominator INT NOT NULL, 
                Numerator INT NOT NULL, 
                PRIMARY KEY (`ID`)
                )
        ''')
        print("Table created successfully: gcse_eng_maths")

        # Insert DataFrame to Table: iterate
        totalRowCount = 0
        for row in dataFrame.itertuples():
            
            #PRINT EACH ROW VALUES
            print(tuple(row))
            #print(row.Measure)
            
            # Make sure to use parameter marker %s only
            sqlInsertQuery = ("INSERT INTO gcse_datastore.gcse_eng_maths"
                "(Measure, Ethnicity, Ethnicity_type, Time, Time_type, Geography, Geography_type, Geography_code, Gender, Value, Value_type, Denominator, Numerator) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                
            inputRowData = (row.Measure, 
                        row.Ethnicity,
                        row.Ethnicity_type,
                        row.Time,
                        row.Time_type,
                        row.Geography,
                        row.Geography_type,
                        row.Geography_code,
                        row.Gender,
                        row.Value,
                        row.Value_type,
                        row.Denominator,
                        row.Numerator)
                        
            cursor.execute(sqlInsertQuery, inputRowData)
            totalRowCount+=cursor.rowcount
            conn.commit()
        print("Record inserted successfully into table: ", totalRowCount, "\n")
    
    except mysql.connector.Error as error:
        print("Failed to insert records into table: {}".format(error), "\n")
    finally:
        disconnectToSql(conn, cursor)

# Step: Load csv dataframe to database
def groupDataStore():
    try:
        conn, cursor = connectToSql()
        
        # Create a new Table
        cursor.execute('DROP TABLE IF EXISTS gcse_datastore.temp_groupedData;')
        cursor.execute('''
            CREATE TABLE gcse_datastore.temp_groupedData(
                geography VARCHAR(50),
                ethnicity VARCHAR(50),
                gender VARCHAR(50),
                TotalValue INT
                -- Add indexing where necessary to deal with large data sets
                -- INDEX `GEOGRAPHY_ID`(`geography` ASC) ,
                -- INDEX `ETHNICITY_ID`(`ethnicity` ASC) 
            );
        ''')
        print("Table created successfully: temp_groupedData")
        
        sqlGroupedQuery = """INSERT INTO gcse_datastore.temp_groupedData
            SELECT geography, 
                ethnicity,
                gender,
                SUM(denominator) AS TotalValue
            FROM gcse_datastore.gcse_eng_maths
            WHERE ethnicity <> 'All' AND geography <> 'All - England'
            GROUP BY geography, ethnicity, gender
            ORDER BY 1,2,3;"""
        print(sqlGroupedQuery, "\n")
        
        cursor.execute(sqlGroupedQuery)        
        conn.commit()
        print("Record inserted successfully into table: ", cursor.rowcount)
        print("Total number of rows in table: ", cursor.rowcount, "\n")  
        
        # get all records: Cross check
        cursor.execute("SELECT * FROM gcse_datastore.temp_groupedData")
        records = cursor.fetchall()
        for row in records:
            print("geography = ", row[0])
            print("ethnicity = ", row[1])
            print("gender  = ", row[2])
            print("TotalValue  = ", row[3], "\n")
        
    except mysql.connector.Error as error:
        print("Failed to groupDataStore: {}".format(error), "\n")
    finally:
        disconnectToSql(conn, cursor)    
    
# Step: Pivot the rows by transposing rows to column: Using the aggregate functions with the CASE expression 
def transposeAndConstructJsonObject():
    try:
        conn, cursor = connectToSql()
        
        # Create a new Table
        cursor.execute('DROP TABLE IF EXISTS gcse_datastore.temp_transposedData;')
        cursor.execute('''
            CREATE TABLE gcse_datastore.temp_transposedData(
                geography VARCHAR(50),
                ethnicity VARCHAR(50),
                `All` INT, -- note: It needs to be in single quotes, as we are using a mysql reserved keyword
                Boys INT,
                Girls INT,
                JSONContent VARCHAR(100),
                FileName VARCHAR(100),
                RelativeFilePath VARCHAR(100)
                -- Add indexing where necessary to deal with large data sets
                -- INDEX `GEOGRAPHY_ID`(`geography` ASC) ,
                -- INDEX `ETHNICITY_ID`(`ethnicity` ASC) 
            );
        ''')
        print("Table created successfully: temp_transposedData")
        
        sqlGroupedQuery = """INSERT INTO gcse_datastore.temp_transposedData
            SELECT 
            t.geography
            , t.ethnicity
            , SUM(CASE WHEN t.gender = 'ALL' THEN t.TotalValue else 0 END) AS 'All'
            , SUM(CASE WHEN t.gender = 'Boys' THEN t.TotalValue else 0 END) AS 'Boys'
            , SUM(CASE WHEN t.gender = 'Girls' THEN t.TotalValue else 0 END) AS 'Girls'
            -- If you need a nested JSON Array Object, you can join JSON_OBJECT with JSON_ARRAYAGG as below:
            -- , JSON_ARRAYAGG(JSON_OBJECT(t.gender, t.TotalValue)) AS 'JSONContent'
            , JSON_OBJECTAGG(t.gender, t.TotalValue) AS 'JSONContent'
            , CONCAT(t.geography, '-', t.ethnicity,'.json') AS 'FileName'
            -- create nested folder stuucture under current path
            , CONCAT('./', t.geography, '/', t.ethnicity, '/', CONCAT(t.geography, '-', t.ethnicity,'.json')) AS 'RelativeFilePath'
            FROM gcse_datastore.temp_groupedData AS t
            GROUP BY t.geography, t.ethnicity;	
        """
        print(sqlGroupedQuery, "\n")
        
        cursor.execute(sqlGroupedQuery)        
        conn.commit()
        print("Record inserted successfully into table: ", cursor.rowcount)

    except mysql.connector.Error as error:
        print("Failed to transposeAndConstructJsonObject: {}".format(error), "\n")
    finally:
        disconnectToSql(conn, cursor)   

# Step: Create json file for all the datastore
def createJsonFileByBatchProcess():
    try:
        conn, cursor = connectToSql()
        
        BATCH_SIZE = 3
        currentOffset = 0
        
        cursor.execute("SELECT COUNT(*) FROM gcse_datastore.temp_transposedData")
        result = cursor.fetchone() # Get the result of the query
        totalRecordSize = result[0] # The result is a tuple with one element, which contains the count       
        print("totalRecordSize for creating json files : ", totalRecordSize, "\n") 
        
        # loop through by batch processing
        while currentOffset < totalRecordSize:
        
            # Read transposed records
            # sqlRetriveTransposedDataQuery = """SELECT JSONContent, FileName, RelativeFilePath FROM gcse_datastore.temp_transposedData"""
            sqlRetriveTransposedDataQuery = "SELECT JSONContent, FileName, RelativeFilePath FROM gcse_datastore.temp_transposedData LIMIT %s OFFSET %s "  %(BATCH_SIZE, currentOffset)
            print(sqlRetriveTransposedDataQuery, "\n")
            
            cursor.execute(sqlRetriveTransposedDataQuery)        
            print("Current records for batch processing : ", cursor.rowcount)
            
            transposedRecords = cursor.fetchall()
            for row in transposedRecords:
                createIndividualJsonFile(row)
                
            currentOffset = currentOffset + BATCH_SIZE 
         
    except mysql.connector.Error as error:
        print("Failed to constructJsonFileByBatchProcess: {}".format(error), "\n")
    finally:
        disconnectToSql(conn, cursor)       

# Step: Create individual json file 
def createIndividualJsonFile(row):
    print("Inside createIndividualJsonFile()", "\n")
    try: 
        # Needs commenting out
        print(row)
        print("JSONContent = ", row[0])
        print("FileName = ", row[1])
        print("RelativeFilePath  = ", row[2], "\n")
        
        JSONContent = row[0]
        fileName = row[2]
        
        os.makedirs(os.path.dirname(fileName), exist_ok=True)
        with open(fileName, "w") as file:
            file.write(JSONContent)
            file.close()
            
    except FileExistsError:
        print("Folder %s already exists" % fileName)  
    except OSError as exc: # Guard against race condition
        if exc.errno != errno.EEXIST:
            raise            
    except:
        print("Failed to createIndividualJsonFile: {}".format(error), "\n")
        
# Step: Main start of the program     
def main():
    
    print("Main program kick started", "\n")
    
    displayData()
    loadCsvToDatabase()
    groupDataStore()
    transposeAndConstructJsonObject()
    createJsonFileByBatchProcess()
    
    # place holder for future code
    pass
    
    print("Main program ended", "\n")
    
if __name__ == "__main__":
    main()    