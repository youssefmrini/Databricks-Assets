# Databricks notebook source
# MAGIC %md
# MAGIC # General Demo Notebook

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Create a fake DataFrame
data = {'Category': ['A', 'B', 'C', 'D', 'E'],
        'Values': np.random.randint(0, 100, 5)}
df = pd.DataFrame(data)

# Create a bar plot using Matplotlib
plt.title('Gender Details')
plt.bar(df['Category'], df['Values'])
plt.show()

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Create a gender DataFrame
gender_counts = {'Gender': ['Male', 'Female'],
                 'Count': [38, 45]}
gender_df = pd.DataFrame(gender_counts)

# Create a pie chart using Matplotlib
plt.pie(gender_df['Count'], labels=gender_df['Gender'], autopct='%1.1f%%')
plt.title('Gender Distribution')
plt.show()