# DenverCrimeVsPopulation
In this project we are trying to analyze how crime in denver changes with attributes of population.

# Data Set

The datasets used here are
  1.  Crime dataset denver city = http://data.denvergov.org/download/gis/crime/csv/crime.csv
  2.  Census dataset = "http://data.denvergov.org/download/gis/census_neighborhood_demographics_2010/csv/census_neighborhood_demographics_2010.csv"  


# Approach

After creating a pandas dataframe, data is cleaned. And then it is pushed into a database. sqlite3 package is extensively used in the project to execute sql queries.
Various visulazation methods are used to find correlations.

#Deeper Analysis
To analyis effect of weather on crime data, weather data is scraped from a website and is used along with crime data.
To see effect of weather and also to continue this project further, weather.py should be run first then lines related to weather in the main program DenverCrime.py can be commented out
