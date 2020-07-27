# Analytics

This part of the project was divided in 4 notebooks:
* 1-EDA - Exploratory Data Analysis: It's used to start the exploratory data, identify the quality of the dataset, such as there is missing values, wrong measurements and the univariate statistcs.  
* 2-Transform_data: This notebook is used to gather and transform differents features that will be analyzed in the next step.  
* 3-Data_Analysis: Here we continue the exploratory analysis, but with a little more in-depth statistics, getting the correlation of all the features, Decomposing of the time series into trend, seasonal and residual, and applying the statistical hypothesis testing to ensure the non-stationarity.  
* 4-Models_comparison: It will compare the regression models, such as Linear Regression, Ridge Regression, Sarimax and Deep Learning LSTM Model.  

### Datasets

ITUB4.csv - Time series of ITUB4 share prices.  
features_data.csv - All variables gathered.  
features_Ibovespa.csv - Time series of Ibovespa points.  
features_USD_BRL.csv - Time series of dollar price in reais.  
Normalized_data.csv - All variables gathered and the features scaled.  
itau_news_tex.csv - Itaú's news and details about it.  


### Columns
The datasets used in this stage have the same fields, but bilingual. So, to simplify the explanation I divided using "/", but this represents the same meaning.

Date/Data - It represents the day for the values in the time series.  
Close/Ultimo - It's the last value of the day for the variable, The ITUB4 Close is the target variable.  
Open/Abertura - It'S the first value of the day for the variable.  
High/Maxima - The highest value of the day for that variable.  
Low/Mínima - The lowest value of the day for that variable.  
Vol - Volume is simply the number of traders of a stock over a given period of time.  
Var - It's the variation of the value during the day.  
Link - It's the link where the news was extracted.  
Class - It's the scenarios of the news, it's split in positive(optimistic), negative(pessimistic) and neutral.  
DayofWeek - Numeric Day of week for the time series values.  
Weekofyear - Numeric week of year for the time series values.  
Quarter - Quarter of year, three-month period.  
Month - Numeric month of year.  
lag_1 - Lagging time series Closing ITUB4 values by one day.   
lag_2 - Lagging time series Closing ITUB4 values by two days.  
lag_3 - Lagging time series Closing ITUB4 values by three days.  


## Preparing the environment

In this project I'm using Jupyter Lab with python 3 on Ubuntu.

To ensure that all notebooks run smoothly, I recommend to use a Virtual Environments (Venv) so that we can isolate the packages used in this project with your packages, mainly to avoid version errors for each lib. It's very simple, let's go.

1. Working with a virtualvenv:

Let's upgrade the virtualenv.

    $ sudo pip install --upgrade virtualenv

2. Creating the virtualenv in this repository folder:

After clone this repository go to Analytics folder to create the Venv for this part of the project.

```
    $ cd stock_market_prediction/Analytics
    $ virtualenv venv
    $ source venv/bin/activate
```

* *to deactivate the venv:* `(venv) $ deactivate`  


## Using pip

The .txt contain all packages required to run the notebooks, thus to install it just run the command below. I prefer pip to anaconda, because I have more control about my environments, but if you prefer you can try use conda instead pip.

    (venv) $ pip install --upgrade -r requirements.txt


## Starting Jupyter Lab
Now we have installed all the requeriments we can use Jupyter lab to run the notebooks:

    (venv) $ jupyter lab



**Note:** If you have problems with Jupyter lab, I recommend you to see this Website:
* https://datawookie.netlify.com/blog/2017/06/setting-up-jupyter-with-python-3-on-ubuntu/


Congratz! Now you can run all the notebooks without a problem! :D

