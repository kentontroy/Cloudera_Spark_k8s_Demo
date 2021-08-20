from impala.dbapi import connect
from impala.util import as_pandas
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

st.set_page_config(layout="wide")

st.write(
"""
## Baltimore Crime Data - CML and Kudu Demo
""")

IMPALA_HOST = "cdp.cloudera.com"
IMPALA_PORT = 21050
IMPALA_USER = "etl_user"
IMPALA_AUTH = "GSSAPI"
KUDU_TABLE = "default.bpd_crime_data"


@st.cache(persist=True, suppress_st_warning=True)
def load(sql: str):
  conn = connect(host=IMPALA_HOST, port=IMPALA_PORT, user=IMPALA_USER, auth_mechanism=IMPALA_AUTH)
  cursor = conn.cursor()
  cursor.execute(sql)
  return as_pandas(cursor)


st.sidebar.write("Filter by year")
yearCheckbox = {}
for y in ["2011", "2012", "2013", "2014", "2015", "2016"]:
  yearCheckbox[y] = st.sidebar.checkbox(y)

filterCond = []
for c in yearCheckbox.keys():
  if yearCheckbox[c]:
    filterCond.append("crimeyear={0}".format(c))   

i = 1
yearSql = ""
for ys in filterCond:
  if i == 1:
    yearSql = "WHERE {0} ".format(ys)
  elif (i > 1 and i < len(yearSql)):
    yearSql = yearSql + "OR {0} ".format(ys)
  else:
    yearSql = yearSql + "{0}".format(ys)
  i = i + 1


sql = "SELECT * FROM {0} {1} LIMIT 100".format(KUDU_TABLE, yearSql)
st.markdown("_{0}_".format(sql))
df = load(sql)
st.dataframe(df)


