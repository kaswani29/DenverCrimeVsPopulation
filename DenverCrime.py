#Author:
# Krishna Aswani, Junghwan Min, Young Gee Kim, Joseph Guillen,  Prasad Seemakurthi

import pandas as pd # importing package, will be using "pd" to refer it
import urllib
import sqlite3 as sql
import pandas.io.sql as pd_sql

#This part downloads file from urls without any manual intervention 
##Downloading Crime.csv file from url to working directory
#testfile=urllib.URLopener()
#testfile.retrieve("http://data.denvergov.org/download/gis/crime/csv/crime.csv")
#testfile.retrieve("http://data.denvergov.org/download/gis/census_neighborhood_demographics_2010/csv/census_neighborhood_demographics_2010.csv")
#creating a dataframe "crime_df" & "census_df"

crime_df = pd.read_csv('crime.csv', header=0)
census_df = pd.read_csv ('census_neighborhood_demographics_2010.csv', header = 0)

#Renaming Variables in census file
census_df.rename(columns={'NBRHD_NAME':'NEIGHBORHOOD_ID'}, inplace=True)


#Getting info about the contents of files for data cleaning purpose, run one at a time
crime_df.head(3) #printing top three values

crime_df.info() # getting info about the valeus stored and if their is any null value
crime_df.describe() # calculating mean median etc, this can be used to get outliers

census_df.head(3) #printing top three values

census_df.info() # getting info about the valeus stored and if their is any null value
census_df.describe() # calculating mean median etc, this can be used to get outliers

#####Importing weather data, this part was used for even deeper analysis with weather
##To use this first, run WeatherParsing.py
#data = pd.read_fwf("weather_1.txt")
#weather_df = pd.DataFrame(data)   #Reading data and converting to a Dataframe


###Data cleaning and Exploratory Data Analysis and Plotting weather data using scrapped weather data
#weather_df.columns = ['Date','Temp','Precp'] #Assigning the names to the Df
#weather_df['Precp'][weather_df.Precp == 'T'] = np.nan #Converting T (missing values) to NAN
#weather_df[['Temp', 'Precp']] = weather_df[['Temp', 'Precp']].astype(float)  #typecasting string to floating point values
#weather_df.plot(subplots=True, figsize=(10,20)); #Checking for outliers
#weather_df[['Temp', 'Precp']] = weather_df[['Temp', 'Precp']].astype(float)  #typecasting string to floating point values


##Exporting to csv
#weather_df.to_csv("weather_df.csv")

#Creating database from sql , using commit and large timeout to prevent database lockdown

conn = sql.connect('project.db', timeout= 3000000)
cursor = conn.cursor()

crime_df.to_sql("crime", conn, flavor='sqlite', if_exists='replace', index=True, index_label=None)
census_df.to_sql("census", conn, flavor='sqlite', if_exists='replace', index=True, index_label=None) 
#weather_df.to_sql("weather", conn, flavor='sqlite', if_exists='replace', index=True, index_label=None)



#Adding new attributes "Clean_Date" & "Clean_time" in requred sqlite format
#in the required format & Cleaning NEIGHBORHOOD_ID for merging 

cursor.execute("ALTER TABLE crime ADD COLUMN Clean_Date text(8);")
conn.commit()

cursor.execute(" ALTER TABLE crime ADD COLUMN Clean_Time text(6);")
conn.commit()

cursor.execute( "update crime set Clean_Date = substr(FIRST_OCCURRENCE_DATE, 1,10)")
conn.commit()

cursor.execute( "update crime set Clean_Time = substr(FIRST_OCCURRENCE_DATE, 12,5)")
conn.commit()

cursor.execute("UPDATE census SET NEIGHBORHOOD_ID = lower(NEIGHBORHOOD_ID)")
conn.commit()

cursor.execute("UPDATE crime SET NEIGHBORHOOD_ID = replace(NEIGHBORHOOD_ID,'-',' ')")
conn.commit()


#Queries 
##################################################################################################
# Query1 -Top 5 Crime Rate and its corresponding neighborhood name in 2013

sql_1 = pd_sql.read_sql('''SELECT NEIGHBORHOOD_ID, Total_count, POPULATION_2010, Total_count*1.0/POPULATION_2010 AS crime_rate
FROM
(SELECT NEIGHBORHOOD_ID, COUNT(OFFENSE_TYPE_ID) AS Total_count
FROM crime
WHERE REPORTED_DATE
BETWEEN '2013-01-01 00:00:00' AND '2014-01-01 00:00:00'
GROUP BY NEIGHBORHOOD_ID) NATURAL JOIN census
ORDER BY crime_rate DESC
LIMIT 5''',conn)

print ("Top 5 Crime Rates and their respective neighborhoods \n \n")
sql_1

#Exporting to csv
sql_1.to_csv("sql_1.csv")
##################################################################################################
# Query2 -Finding Crime rate for neighborhood where percentage of senior citizen is highest

sql_2 = pd_sql.read_sql('''SELECT NEIGHBORHOOD_ID, count(OFFENSE_TYPE_ID) AS No_of_Crimes, POPULATION_2010, COUNT(OFFENSE_TYPE_ID)*1.0/POPULATION_2010 AS Crime_rate
FROM 
crime NATURAL JOIN 
(SELECT NEIGHBORHOOD_ID, PCT_65_PLUS, POPULATION_2010  
FROM census
GROUP BY NEIGHBORHOOD_ID
ORDER BY PCT_65_PLUS
LIMIT 1)
WHERE REPORTED_DATE
BETWEEN '2013-01-01 00:00:00' AND '2014-01-01 00:00:00'
GROUP BY NEIGHBORHOOD_ID''', conn)

print ("Crime rate for neighborhood having max percent of Senior Citizens")

sql_2

#Exporting to csv
sql_2.to_csv("sql_2.csv")
##################################################################################################
# Query3 -Finding Crime rate for neighborhood where percentage of senior citizen is lowest

sql_3 = pd_sql.read_sql('''SELECT NEIGHBORHOOD_ID, count(OFFENSE_TYPE_ID), POPULATION_2010, COUNT(OFFENSE_TYPE_ID)*1.0/POPULATION_2010 AS Crime_rate
FROM 
crime NATURAL JOIN 
(SELECT NEIGHBORHOOD_ID, PCT_65_PLUS, POPULATION_2010  
FROM census
GROUP BY NEIGHBORHOOD_ID
ORDER BY PCT_65_PLUS DESC
LIMIT 1)
WHERE REPORTED_DATE
BETWEEN '2013-01-01 00:00:00' AND '2014-01-01 00:00:00'
GROUP BY NEIGHBORHOOD_ID''', conn)


print ("Crime rate for neighborhood having minimum percent of Senior Citizens")

sql_3

#Exporting to csv
sql_3.to_csv("sql_3.csv")

#################################################################################################################
# Query4 -for finding the number of bank robberies in each neighborhood


sql_4 = pd_sql.read_sql("""SELECT Neighborhood_ID, count(Offense_Type_ID) AS total 
FROM crime 
WHERE (Clean_Date  BETWEEN '2013-01-01' AND '2014-01-01') 
AND Offense_Type_ID = 'robbery-bank' 
GROUP BY Neighborhood_ID 
ORDER BY total DESC""", conn)

#Plotting Neighborhood vs Number of Bank Robberies
sql_4.columns = ['Neighborhood','Number of Bank Robberies']
sql_4 = sql_4.set_index('Neighborhood')
sql_4.plot(kind = 'bar')

#Exporting to csv
sql_4.to_csv("sql_4.csv")

##########################################################################################################################
# Query5-7 -for finding number of instances of larceny, drug/alcohol, and white collar crime in each district (Query works, need to fix labels and legend)

#Larcency
sql_5 = pd_sql.read_sql("""SELECT District_ID, count(Offense_Category_ID) AS totalL 
FROM Crime 
WHERE Offense_Category_ID = 'larceny' 
GROUP BY District_ID""",conn)

# Drug/Alcohol
sql_6 = pd_sql.read_sql("""SELECT District_ID, count(Offense_Category_ID) AS totalDA 
FROM Crime 
WHERE Offense_Category_ID = 'drug-alcohol' 
GROUP BY District_ID""",conn)

#white collar crime
sql_7 = pd_sql.read_sql("""SELECT District_ID, count(Offense_Category_ID) AS totalWC 
FROM crime 
WHERE Offense_Category_ID = 'white-collar-crime' 
GROUP BY District_ID""",conn)

#merged
sql_merge=pd.DataFrame({"District":sql_5["DISTRICT_ID"], "Larceny":sql_5["totalL"],
                        "Drug/Alcohol":sql_6["totalDA"],"White-Collar Crime":sql_7["totalWC"]})
sql_merge = sql_merge.set_index('District')

#plotting merge
sql_merge.plot(kind='barh', stacked=True)

#Exporting to csv
sql_merge.to_csv("sql_merge.csv")

#################################################################################################################
# Query8 -for finding the number of crimes in each district per month

#reading databse
sql_8= pd_sql.read_sql("""SELECT substr(Clean_Date,6,2) AS Month, District_ID, count(District_ID) AS total 
FROM crime 
WHERE Clean_Date  BETWEEN '2013-01-01' AND '2013-12-31' 
GROUP BY Month, District_ID""",conn)

#plotting graph
sql_8.columns = ['Month', 'District_ID', 'Total Crimes']
sql_8.groupby('District_ID').plot(x='Month', y='Total Crimes', ylim=[0,1600],  title='Crimes per Month by District')

#Exporting to csv
sql_8.to_csv("sql_8.csv")

####################################################################################
# Query9 - To find the correlation with crime called criminal mischief of motor vehicle and 
# young age. The result shows that the neighborhood where the crime was most committed
# has much higher percentage of adolescent population than the average.

sql_9 =  pd_sql.read_sql('''SELECT NEIGHBORHOOD_ID, count, PCT_LESS_18
FROM
(SELECT NEIGHBORHOOD_ID, count(OFFENSE_TYPE_ID) AS count
FROM crime
WHERE REPORTED_DATE
BETWEEN '2013-01-01 00:00:00' AND '2014-01-01 00:00:00'
AND OFFENSE_TYPE_ID = "criminal-mischief-mtr-veh"
GROUP BY NEIGHBORHOOD_ID
ORDER BY count DESC
LIMIT 1)
NATURAL JOIN census
GROUP BY NEIGHBORHOOD_ID
ORDER BY PCT_LESS_18 DESC''',conn)

print ("Count for criminal mischief of motor vehicle and percentage of adolscent ")

sql_9

#Exporting to csv
sql_9.to_csv("sql_9.csv")


####################################################################################
#To find number of crimes of particular type in each neighborhood, raw_input from user

print ("Please choose a offense type from list to calculate its frequency in each neighborhood: \n")
print ("""\ttheft-items-from-vehicle
        theft-of-motor-vehicle
        criminal-mischief-mtr-veh
        theft-other
        burglary-residence-by-force
        criminal-mischief-other
        burglary-residence-no-force
        theft-bicycle
        traf-other
        theft-parts-from-vehicle
        theft-shoplift
        assault-simple
        criminal-mischief-graffiti
        theft-from-bldg
        aggravated-assault
        burglary-business-by-force
        criminal-trespassing
        assault-dv
        robbery-street
        liquor-possession \n \n""")

Usr_Inpt = str(raw_input("\n"))


sql_10 = pd_sql.read_sql("""SELECT Neighborhood_ID, count(Offense_Type_ID) AS total 
FROM crime 
WHERE (Clean_Date  BETWEEN '2013-01-01' AND '2014-01-01') 
AND Offense_Type_ID =""" +"'" + Usr_Inpt +"'"+"""
GROUP BY Neighborhood_ID 
ORDER BY total DESC""", conn)


print "number of crimes for {} in each neighborhood are:".format(Usr_Inpt)

sql_10

#Exporting to csv
sql_10.to_csv("sql_10.csv")


##############################################################
# Integrity Checks and key constraints
#conn.execute('''CREATE TABLE weather2
#(DATE DATE PRIMARY KEY NOT NULL,TEMP INT, PRECP INT );''') ##Creating a table with key constraints
#
#conn.execute('''INSERT INTO weather2 SELECT * FROM weather''')

#############################################################
## Plotting some other visualizations of crime
count_crime =crime_df['DISTRICT_ID'].value_counts() ## finding total no of crimes per districts
count_crime = pd.DataFrame(count_crime)             ## converting serier obj to data frame
count_crime['counts'] = count_crime.index
count_crime.index = range(len(count_crime.index))
count_crime.columns = ['Count','DISTRICT_ID']

viz1 = pd.merge(crime_df,count_crime, on = "DISTRICT_ID", how = 'outer')
viz2 = pd.merge(viz1,census_df, on ='NEIGHBORHOOD_ID', how ='outer' )


##### plotting crimes per district
viz2.plot(x="DISTRICT_ID", y = "Count" , kind = 'scatter')

#Exporting to csv
viz2.to_csv("viz2.csv")

