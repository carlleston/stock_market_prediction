# stock_market_prediction

This is a Final Capstone Project of Coursera course Advanced Data Science create by IBM.

This project aims to predict the Itau (a Brazillian Bank) stock prices. Knowing that economics is a social science and influence all the the market around, we will use Itau News getting in Brazillian leading investment site as a feature of the models, and anothers continous exogenous variables such as Dollar price, Ibovespa Values and the Itaú open share price values.

So it has two mains model that will be use, a regression model and a classifier model both of them is supervised learn. The Regression model is used to create a forecast of the time series and predict the Close value stock market and the Classifier will use to create a feature that will be on of 5 indepent variables of the regression model.

The entire project is divided into 2 ways, the analytics and deploy. In the analytics branch we will study all data source, data exploratory, getting insights of the data, testing differents regression and classifier models and pick the best one. All of this way will create in Jupyter Notebook, so we don't need to use any cloud service, thus it will be local. The deploy method will use a lot of IBM Cloud services, in there will be the ETL (Extract, transform and load) of the data, the cluster for processing the models and all the storage to be avaliable to the stakeholders consume an API.

To manager this project it was used the Analytics Solution Unified Method, It like Crisp-DM (the standard for data science project) but extends with  tasks and activities on infrastructure, operations, project, deploymend and adds templates and guidelines to all the tasks (with some steps losing in the middle way, but alway trying to get them).

![](https://github.com/carlleston/stock_market_prediction/blob/master/asum-process-detail.jpg)



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





