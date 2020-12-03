import pandas as pd
import numpy as np

df : pd.DataFrame = pd.read_csv("oren_final_output.csv", header=None)

df = df[[1,2,3,4,5]]

df = df.sum(axis=1)

s = np.sum(np.array(df))

print(s)