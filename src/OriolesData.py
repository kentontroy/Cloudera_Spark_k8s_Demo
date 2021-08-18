import pandas as pd
import glob
import os

# Schema from baseball-almanac.com
# gameno,gamedate,opponent,outcome

path = r"/home/centos/data/"
files = glob.glob(os.path.join(path, "orioles*.csv"))
df = pd.concat([pd.read_csv(f, index_col=None, header=0) for f in files], axis=0, ignore_index=True, sort=False)
df["gameloc"] = df["opponent"].apply(lambda x: "AWAY" if x.startswith("at", 0) else "HOME")

print(df)

output = r"/home/centos/data/orioles-2011-2016.csv"
df.to_csv(output, sep="|", encoding="utf-8")
