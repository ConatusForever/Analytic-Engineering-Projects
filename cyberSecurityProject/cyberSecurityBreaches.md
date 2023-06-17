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