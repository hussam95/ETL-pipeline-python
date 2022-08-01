# ETL-pipeline-python

## Background

ETL is the backbone of any data related task. ETL is an acronym for Extract, Transform, and Load. Real world data is often clumsy and requires certain degree of preprocessing before it can be fed to end applications. ETL pipelines are designed to achieve the goal of data preprocessing. They take in raw data and spit out clean data stored in SQL or NoSQL databases.

![Screenshot](etl.png)

## ETL Explanation

Every ETL pipeline has three stages:

1. Data extraction from some source/s (say flat files or some database)

2. Data transformation using some tools

3. Loading of data in SQL or NoSQL databases

The loaded data is then used in end applications. One possible use case is using the cleaned data for business reporting using tools like [Tableau](https://www.tableau.com/) and [MS Power BI](https://powerbi.microsoft.com/).

## Explaining Work in Repo

This repository builds custom ETL pipelines in Python. Two data pipelines have been designed using the files in this repository:

1. In the first ETL pipeline, raw data is extracted from flat files using Pandas. The extracted data is then transformed using Pandas and Numpy. Finally, the transformed data is loaded in a NoSQL database, MongoDB, hosted on [MongoDB Atlas](https://www.mongodb.com/atlas/database). The data loading process is carried out with the help of [PyMongo](https://pymongo.readthedocs.io/en/stable/) — MongoDB's recommended driver for Python.
![Screenshot](etl_mongo.jpg)

2. In the second ETL pipeline, data is extracted from flat files using Pandas. Data is then transformed according to the requirements using Pandas and Numpy. Finally, the data is loaded in a SQL database, SQL Server, hosted locally. MS SQL Server's recommended connector for Python, [pyodbc](https://pypi.org/project/pyodbc/), has been used in data loading step. **Due diligence has been exercised in ensuring compliance with key database engineering concepts such as [database normalization](https://docs.microsoft.com/en-us/office/troubleshoot/access/database-normalization-description).**
![Screenshot](etl_sqlserver.jpg)
