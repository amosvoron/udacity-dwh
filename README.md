# Data Warehouses on AWS
This is a data warehouse project for the Udacity Data Engineering Nanodegree program.

## Project Overview
In this project we'll create an ETL pipeline that extracts data from the Amazon's S3 store and loads it into the Amazon Redshift PostreSQL database. We'll create the Amazon Redshift cluster using AIC (*architecture as code*), then we create tables, and finally we'll execute ETL process. The tables we'll be of two types: the *staging tables* (2 tables) and *analytics tables* (5 tables). The staging tables will reflect the *data source* which consists of *songs data* and *log events* data simulating the data of a music streaming website like Spotify. The analytics tables are designed as a star schema having a central fact table

 - *songplays*

and 4 dimension tables:

 - *time*
 - *users*
 - *songs*
 - *artists*

The analytics tables will be optimized for the partitioning on the Redshift cluster by *distribution style* and *sorting key* strategy. Since we expect the analytics querying to be focused on song analysis and the *songs* table is also quite big (up to 1 mio rows - our table is just a small portion of the original songs table) we'll choose the *songs* table for KEY distribution style strategy. This means that the songs rows with the same key will be placed in the same slice which will reduce *shuffling* when joining the songs. For the rest of dimension tables the AUTO distribution style will be used leaving the decision how to best partition data to Redshift. Apart from distribution style options We'll also choose the *sorting key* option for columns that are frequently used in sorting (like date).

## Installation
### Clone
```sh
$ git clone https://github.com/amosvoron/udacity-dwh.git
```

### Instructions
To create tables run the following command:
```sh
$ python create_tables.py
```

To execute ETL process run the following command:
```sh
$ python etl.py
```

## Repository Description

```sh
- amazon_redshift_cluster.py           # AIC code for cluster management   
- analytic_queries.ipynb               # analitics query examples 
- create_tables.py                     # python code for table creation
- dwh.cfg                              # configuration file
- etl.ipynb                            # ETL demo and testing notebook
- etl.py                               # ETL python code
- sql_queries.py                       # SQL queries
- README.md                            # README file
- LICENCE.md                           # LICENCE file
```

## License

MIT
