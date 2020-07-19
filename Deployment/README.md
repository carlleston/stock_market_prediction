# Deployment

This part of the project was divided into 2 phases:
1 - IBM Watson Studio: The Extract, Transform, load (ETL) was used together with the function JobSchedule, that run every 8 am from mondays to fridays, the Watson Studio was choice due the lite plan, it has 50 capacity unit-hours monthly, thus it's enough to run the ETL 20 times in the month and it's very simple to connect with the IBM Cloud Object Storage through the Access Tokens.
* A-ETL_TS - Using the yfinance (yahoo finance API) to get the Dollar values, Ibovespa points and the ITUB4 time series and storage on IBM COS
* B-ETL_news_Studio - Using the Google Search API to get the link of Itaú's news, Crawler Scrapy to obtain the news content, pre-processing the text and use the RNN Deep Learning Classifier to tag the news in scenarios.

2 - IBM Analytics Engine: Through a spark-submit script it will trigged executing the ln_model.py (python script) into a Cloud Cluster with Apache Spark and hdfs, where there's some advantages in Big Data Processing. We can trigger this using a ssh connection, so after to connection to the IBM Analytics Engine through ssh and prepared the Cluster and datas, we can execute:

```
spark-submit --master yarn --executor-memory 1g --deploy-mode client --name ln_model --conf "spark.app.id = ln_model" /home/wce/clsadmin/ln_model.py hdfs://chs-fvk-045-mn002.us-south.ae.appdomain.cloud:8020/user/clsadmin/ts_data.csv hdfs://chs-fvk-045-mn002.us-south.ae.appdomain.cloud:8020/user/clsadmin/itau_news.csv
```

Explain the spark-submit:

--master: Used to specify the master address using yarn mode.
--executor: Specifies the mount of memory to use per executors process.
--deploy-mode: Deploy the driver locally as an external client.
--name: Specifies the application's name
--conf: Specifies the spark configuration property.
script filepath: Indicates the file containing the python code to execute.
sys.arg[1] filepath: Name node, host name for the management slave.
sys.arg[2] filepath: Name node, host name for the management slave.


This video helped me a lot: https://www.youtube.com/watch?v=W_8cNI9Il98

### Datasets

ts_data.csv - Time series of ITUB4 share prices and independent variables.
itau_news.csv - Itaú's news and details about it.


### Columns
The datasets used in this stage have the same fields, but bilingual. So to simple the explanation I divided using "/", that is represent the same meaning.

Date - It represents the day for the values in the time series.
Itau_Close - It's the last value of the day for the variable, the ITUB4 Close is the target variable.
Itau_open - It's the first value of the day for the variable ITUB4.
BVSP_open - It's the first value of the day for the variable Ibovespa points.
USDBRL_open - It's the first value of the day for the variable USDBRL.
lag_1 - Lagging time series Closing ITUB4 values by one day. 
lag_2 - Lagging time series Closing ITUB4 values by two days. 
lag_3 - Lagging time series Closing ITUB4 values by three days. 




