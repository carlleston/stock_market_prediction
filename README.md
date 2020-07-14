# stock_market_prediction
This is a Final Capstone Project of Coursera course Advanced Data Science create by IBM.

## Navigation structure in the directory


    ├── stock_market_prediction            # 
    │   ├── Analytics                      # 
    │   |   ├── xxxx                       # 
    │   |   └── yyyy.ipynb                 # 
    │   ├── Deployment                     # 
    │   |   ├── ETLs_IBM          # Cada análise deve estar em uma pasta sepadara por data
    |   |   |    └── Analyze_MMDDAAA.ipynb # Relatório
    │   |   └── ... 
    |   └── ... 


## Introduction

Investing in the financial market has become more popular in Brazil, along with advertisements from brokers and banks, where there are specialists who try to make a conscious use of the money invested. The application of the machine learning algorithm designed to support the financial market has been growing day by day, based on exogenous factors, such as economic, political, health and among other factors linked to important indices that affect a country's market, of which has been helping these experts in making data-based decisions.

Predicting the exact price of a share is a task considered impossible for many experts, however with the know how and years of experience it is possible to predict some fluctuations, whether they are generated by notions and market experience, even, as some say, "gut feeling" .

The economy, as a social science, is not as predictable or easy to understand, being one of the main factors that impact the stock market. says an economist friend that “if you put six economists in a room to debate a country's economy, there will be at least 7 theories”, although funny is the purest reality, and then comes the statistical techniques and algorithms to be able to affirm and explain patterns and certain oscillations that are statistically significant.

The stock market is nothing more than the environment in which publicly traded companies have parts of themselves for sale, the price of each of these partitions fluctuates, generating time series containing periodicity, trends, noises even “traps”, despite a correlation do not imply causality some factors end up having a strong association and can be used to estimate the price of these shares, and this is where regression algorithms come in to help us.

## Use Case

This project has the general purpose (besides the main objective being the learning of an end-to-end machine learning project) to forecast the daily closing price of ITUB4 (Itaú Unibanco) using Itaú news published on the Infomoney website that will be classified by an RNN neural network algorithm generating categorical independent variables in positive (P), negative (N) and neutral (NN) scenarios, and also other independent variables such as the share price at its opening, dollar value in reais and the Ibovespa (which is in São Paulo as well as Dow Jones is in New York).

## Solution

The entire project is divided into 2 ways, the analytics and deployment. In the analytics branch we will study all data source, data exploratory data analysis, getting insights of the data, creating a Recurrent Neural Network News Classifier, testing differents kind regression and pick the best one. All of this way will create in Jupyter Notebook, so we don't need to use any cloud service, thus it will be local. 
* to run everything smoothly, venv will be used for this repository.

The deploy method will use a lot of IBM Cloud services, in there will be the ETL (Extract, transform and load) of the data, the Analytics Engine cluster for processing the models and all the storage to be avaliable to the stakeholders consume an API.

To manager this project it was used the Analytics Solution Unified Method, It's like Crisp-DM (the standard for data science project) but extends with  tasks and activities on infrastructure, operations, project, deploymend and adds templates and guidelines to all the tasks (with some steps losing in the middle way, but I'm always trying to get them :D).

![](https://github.com/carlleston/stock_market_prediction/blob/master/asum-process-detail.jpg)

## Architecture

![](https://github.com/carlleston/stock_market_prediction/blob/master/architecture.jpg)

The cloud architecture of this project is very simple. to avoid data transition rates between different types of cloud computing companies, IBM chose to maintain all services.
Despite having a supposed parallelism in our architecture, I will explain it sequentially.

1 - JobSchedule - ELT news: It has a jobschedule for 8 am where using a Google Search API searches for the word ‘’ Itaú ”within the Infomoney website, in which 8 links will be returned.
 Using Scrapy as a WebCrawler on the links we get, we managed to get the body of the site containing the news from it.
With the RNN model already trained in the Analytics stage, we use the same saved in tf.learn to only classify the news and thus store it in the Cloud Object Storage.

2 - JobSchedule - ELT Time Series: It has a Jobschedule for 8am, where it uses the Yahoo Finance API to extract the entire ITUB4 time series, IBovespa and the dollar price, storing in the Cloud Object Storage in sequence

3 - SparkML Processing: This is where the magic happens, using IBM Analytics Engine, after selecting the model that best fits our purpose in the Analytics stage, a pyspark script was created where it uses SparkML to load the data from the previous steps, create the encoder of the news, and to scale the independent variables, after this pre-processing of the data, the regression model that performs the predictions is trained, saving in the hdfs itself in parquet format. Remembering that the whole process is triggered by Spark-Submit via ssh.

4 - The processed data becoming available for consumption by the stakeholders through API.

*all justifications for technology choices are in the Architectural Decisions Document (ADD)

## Conclusion



