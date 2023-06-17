# A Breached Fortress: Analyzing the Government Agency Cybersecurity Breaches

![image.png](https://www.reliasite.com/wp-content/uploads/2019/08/bigstock-Hacker-Using-Laptop-With-Binar-257453926-e1565109796243.jpg)

# The Background
In a not-so-distant future, a government-owned agency that plays a crucial role in national security and public welfare finds itself entangled in a series of alarming cybersecurity breaches. This agency, responsible for handling sensitive information and critical infrastructure, was once regarded as a fortress of impenetrable security measures. However, recent events have exposed its vulnerabilities and sent shockwaves through the nation.

In order to recover, this agency has tasked you with analyzing data involving its breaches. Entity names have not been disclosed for security purposes. Your mission, should you choose to accept it, is to analyze the data and uncover trends and insights regarding the breaches then add your recommendations. The future of the agency and the nation's security is in your hands.

# Data Description

| Column Name | Description |
| --- | --- |
| **ID** | Entity breached identification number |
| **Name of Covered Entity** | Cover ame for breached entities |
| **State** | State of origin for entity |
| **Business Associate Involved** | Whether or not the breach involved a business associate |
| **Individuals Affected** | Number of people affected by the breach |
| **Breach Start** | Start date of breach |
| **Breach End** | End date of breach |
| **Posted/Updated** | The date in which the breach was posted for open to public |
| **Type of Breach** | The mode of breach|
| **Location of Breached Information** |  The technical device(s) that were compromised |

# Data Profiling and Cleaning

As we take our first step towards helping the agency, we must first understand the data we are working with. We will begin by profiling the data and cleaning it up for analysis. We won't be doing any machine learning in this project, so we will be focusing on the data cleaning and exploratory data analysis (EDA) aspects of the data science process.

```python
#importing libraries and data

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt

df = pd.read_excel('CyberSecurityBreaches.xlsx')
breaches = df.copy()
```
Let's take a look at the data we are working with.

```python
# Inspect the data

print('First 5 rows of the data')
print(tabulate(breaches.head(), tablefmt="pipe", headers="keys"), '\n')
print('Last 5 rows of the data')
print(tabulate(breaches.tail(), tablefmt="pipe", headers="keys"), '\n')
print('Random 5 rows of the data')
print(tabulate(breaches.sample(5), tablefmt="pipe", headers="keys"))
```
First 5 rows of the data
|    |    ID | Name of Covered Entity   | State   | Business Associate Involved   |   Individuals Affected | Breach Start        | Breach End          | Posted/Updated      | Type of Breach   | Location of Breached Information        |
|---:|------:|:-------------------------|:--------|:------------------------------|-----------------------:|:--------------------|:--------------------|:--------------------|:-----------------|:----------------------------------------|
|  0 | 90840 | Entity 1                 | TX      | No                            |                   1711 | 2015-09-13 00:00:00 | 2015-10-15 00:00:00 | 2016-06-29 00:00:00 | Theft            | Paper                                   |
|  1 | 90711 | Entity 2                 | MO      | No                            |                    692 | 2014-07-13 00:00:00 | 2014-07-13 00:00:00 | 2016-05-29 00:00:00 | Theft            | Network Server                          |
|  2 | 90799 | Entity 3                 | AK      | No                            |                    606 | 2015-04-06 00:00:00 | 2015-05-21 00:00:00 | 2016-01-23 00:00:00 | Theft            | Other Portable Electronic Device, Other |
|  3 | 90868 | Entity 4                 | DC      | No                            |                  50000 | 2016-04-07 00:00:00 | 2016-04-07 00:00:00 | 2016-01-23 00:00:00 | Loss             | Laptop                                  |
|  4 | 90611 | Entity 5                 | CA      | No                            |                   2279 | 2014-09-19 00:00:00 | 2014-09-26 00:00:00 | 2016-01-23 00:00:00 | Theft            | Desktop Computer                        | 

Last 5 rows of the data
|      |    ID | Name of Covered Entity   | State   | Business Associate Involved   |   Individuals Affected | Breach Start        | Breach End          | Posted/Updated      | Type of Breach                 | Location of Breached Information   |
|-----:|------:|:-------------------------|:--------|:------------------------------|-----------------------:|:--------------------|:--------------------|:--------------------|:-------------------------------|:-----------------------------------|
| 2105 | 91347 | Entity 2016              | MO      | No                            |                    700 | 2014-08-07 00:00:00 | 2015-03-05 00:00:00 | 2015-11-19 00:00:00 | Theft                          | Other                              |
| 2106 | 91306 | Entity 2017              | IL      | No                            |                   8911 | 2011-10-17 00:00:00 | 2012-08-18 00:00:00 | 2013-05-04 00:00:00 | Hacking/IT Incident            | Other                              |
| 2107 | 91893 | Entity 2018              | NC      | No                            |                   2777 | 2011-12-16 00:00:00 | 2013-03-09 00:00:00 | 2013-12-14 00:00:00 | Theft, Loss                    | Other Portable Electronic Device   |
| 2108 | 91626 | Entity 2019              | OR      | No                            |                    660 | 2014-10-07 00:00:00 | 2016-08-31 00:00:00 | 2017-06-07 00:00:00 | Unauthorized Access/Disclosure | E-mail                             |
| 2109 | 91935 | Entity 2020              | MO      | Yes                           |                    600 | 2013-01-06 00:00:00 | 2013-07-19 00:00:00 | 2013-12-02 00:00:00 | Theft                          | Desktop Computer                   | 

Random 5 rows of the data
|      |    ID | Name of Covered Entity   | State   | Business Associate Involved   |   Individuals Affected | Breach Start        | Breach End          | Posted/Updated      | Type of Breach                 | Location of Breached Information   |
|-----:|------:|:-------------------------|:--------|:------------------------------|-----------------------:|:--------------------|:--------------------|:--------------------|:-------------------------------|:-----------------------------------|
| 1734 | 91634 | Entity 1645              | MI      | Yes                           |                    824 | 2016-03-30 00:00:00 | 2016-12-07 00:00:00 | 2017-01-08 00:00:00 | Unauthorized Access/Disclosure | Paper                              |
| 2069 | 91806 | Entity 1980              | MN      | Yes                           |                  10271 | 2014-12-21 00:00:00 | 2016-05-21 00:00:00 | 2016-07-08 00:00:00 | Hacking/IT Incident            | E-mail                             |
|  986 | 90839 | Entity 215               | WA      | No                            |                   8555 | 2014-05-01 00:00:00 | 2014-09-21 00:00:00 | 2016-04-20 00:00:00 | Other                          | E-mail                             |
| 1533 | 91381 | Entity 1444              | CA      | No                            |                    690 | 2013-05-31 00:00:00 | 2015-01-30 00:00:00 | 2015-11-10 00:00:00 | Other                          | Network Server                     |
|  551 | 90695 | Entity 243               | NY      | No                            |                   8000 | 2015-02-27 00:00:00 | 2015-02-27 00:00:00 | 2016-01-23 00:00:00 | Theft                          | Desktop Computer                   |

This looks ok to me. I don't see anything alarming right off the bat. Lets move forward.

First, we'll inspect the data types of each column. This will help us understand what we're working with.

```python
print(tabulate(breaches.dtypes.reset_index().rename(columns={0:'dtype', 'index':'column'}), tablefmt="pipe", headers="keys"))
```
|    | column                           | dtype          |
|---:|:---------------------------------|:---------------|
|  0 | ID                               | int64          |
|  1 | Name of Covered Entity           | object         |
|  2 | State                            | object         |
|  3 | Business Associate Involved      | object         |
|  4 | Individuals Affected             | int64          |
|  5 | Breach Start                     | datetime64[ns] |
|  6 | Breach End                       | datetime64[ns] |
|  7 | Posted/Updated                   | datetime64[ns] |
|  8 | Type of Breach                   | object         |
|  9 | Location of Breached Information | object         | 

Next, we'll look at the shape of the data. This will help us understand how many rows and columns we're working with.

```python
print(f'There are {breaches.shape[0]} rows and {breaches.shape[1]} columns in the data')
```

There are 2110 rows and 10 columns in the data