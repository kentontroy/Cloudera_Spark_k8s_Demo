from impala.dbapi import connect
from impala.util import as_pandas
from fbprophet import Prophet
import datetime
import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st
import warnings
warnings.simplefilter("ignore")

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
DEMO_ONLY_FILE_PATH = "/home/centos/data/orioles-2011-2016.csv"

##################################################################################################
# Load data from Imapala
##################################################################################################
@st.cache(persist=True, suppress_st_warning=True)
def load(sql: str):
  conn = connect(host=IMPALA_HOST, port=IMPALA_PORT, user=IMPALA_USER, auth_mechanism=IMPALA_AUTH)
  cursor = conn.cursor()
  cursor.execute(sql)
  return as_pandas(cursor)

##################################################################################################
# Limit number of records in the dataframe
##################################################################################################
limit = 10000
limitStr = st.sidebar.text_input("Limit records", "10000")
try:
  limit = int(limitStr) 
except ValueError:
  pass

##################################################################################################
# Create a WHERE clause for filtering by year
##################################################################################################
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

##################################################################################################
# Filter by crime type
##################################################################################################
crimeFilter = st.sidebar.radio("Filter by crime type", ("ALL", "HOMICIDE", "RAPE", "AUTO THEFT", "ARSON"))

##################################################################################################
# Main body execution
##################################################################################################
sql = "SELECT * FROM {0} {1} LIMIT {2}".format(KUDU_TABLE, yearSql, limit)
st.markdown("_{0}_".format(sql))
df = load(sql)
dfCopy = df.copy(deep=True)

if crimeFilter == "HOMICIDE":
  dfCopy.query("description == '{0}'".format("HOMICIDE"), inplace=True) 
elif crimeFilter == "RAPE":
  dfCopy.query("description == '{0}'".format("RAPE"), inplace=True) 
elif crimeFilter == "AUTO THEFT":
  dfCopy.query("description == '{0}'".format("AUTO THEFT"), inplace=True) 
elif crimeFilter == "ARSON":
  dfCopy.query("description == '{0}'".format("ARSON"), inplace=True) 

# Display the Geo-encoded layer
hexagons = pdk.Layer(
  "HexagonLayer",
  dfCopy,
  get_position=["lon", "lat"], 
  auto_highlight=True,
  elevation_scale=10,
  pickable=True,
  elevation_range=[0, 100],
  extruded=True,
  coverage=0.75
)

viewState = pdk.ViewState(longitude=-76.5864, latitude=39.2935, zoom=11, min_zoom=6, max_zoom=20, 
                          pitch=40.5, bearing=-27.36)

# https://docs.mapbox.com/api/maps/styles/
apiKeys={}
apiKeys["mapbox"]="pk.eyJ1Ijoia2VudG9udHJveSIsImEiOiJja3NnYnRzYTAxaXRzMm9udm5rcTVneXFpIn0.CBsFJ2v1T9BQsfnd0tXOhg:"
deck = (pdk.Deck(initial_view_state=viewState, layers=[hexagons], 
                 api_keys=apiKeys,
                 map_provider="mapbox", 
                 map_style="mapbox://styles/mapbox/satellite-streets-v11"))
deckchart = st.pydeck_chart(deck)

# Display sample data
st.dataframe(dfCopy)

##################################################################################################
# Aggregate the crime occurrences by the day
# Coerce bad date values into 'NaT' and then drop the missing ones
##################################################################################################
timestamps = pd.to_datetime(dfCopy["crimedate"], format="%m/%d/%Y", errors="coerce")
dfTimestamps = pd.DataFrame({"datetimestamp": timestamps}, columns = ["datetimestamp"])
dfTimestamps.dropna(axis=0, inplace=True)
dfTimestamps["groupVar"] = dfTimestamps["datetimestamp"].apply(lambda x: str(x))
dfCounts = dfTimestamps.groupby(["groupVar"])["datetimestamp"].count().reset_index(name="y")
dfCounts.rename(columns={"groupVar": "ds"}, inplace=True)
dfCounts["ds"] = dfCounts["ds"].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d"))
st.write("### Aggregated Crime Counts by Day")
st.dataframe(dfCounts)

##################################################################################################
# Use Facebook's Prophet package for time series modeling
# Time series data can change trajectories abruptly.
# The model uses a sparse uniform prior distribution of where those changes can occur and updates
# this prior iteratively per Bayesian inference.
# The key parameter to tune is the changepoint_prior_scale.
# It dictates how many changepoints are assumed when fitting the data.
#
# Bayesian time series analyis with Generalized Additive Models
# y(t)= g(t) + s(t) + h(t) + εt
# g(t): piecewise linear or logistic growth curve for modeling non-periodic changes in time series
# s(t): periodic changes (e.g. weekly/yearly seasonality)
# h(t): effects of holidays (user provided) with irregular schedules
# εt:   error term accounts for any unusual changes not accommodated by the model
#
# https://facebook.github.io/prophet/docs/trend_changepoints.html
##################################################################################################
st.write("### Time Series Analysis")
m = Prophet(changepoint_prior_scale=0.80).fit(dfCounts)
# Forecast predictions for the next 365 days
future = m.make_future_dataframe(periods=365, freq='D')
fcst = m.predict(future)
fig = m.plot(fcst)
st.pyplot(fig)

st.write("### Detect Trends and Seasonal Components")
figTrends = m.plot_components(fcst)
st.pyplot(figTrends)

##################################################################################################
# Visually, it appears that more crime occurs around the baseball and football stadiums
# Try to identify differences in crime rates when the Orioles are playing.
##################################################################################################
st.write("""
   ### Use an outer join to identify when Orioles are playing and when they are not
""")
dfOrioles = pd.read_csv(DEMO_ONLY_FILE_PATH, delimiter="|", index_col=None, header=0)
dfOrioles.query("gameloc == 'HOME'", inplace=True)
dfMerged = pd.merge(left=dfCopy, right=dfOrioles, how="outer", left_on="crimedate", right_on="gamedate")
st.write(dfMerged)
dfOriolesPlaying = dfMerged[dfMerged["gamedate"].notnull()]
dfOriolesNotPlaying = dfMerged[dfMerged["gamedate"].isnull()]

st.write("### Size of master data set: {0}".format(dfMerged.shape[0]))
st.write("### Size when Orioles playing: {0}".format(dfOriolesPlaying.shape[0]))
st.write("### Size when Orioles not playing: {0}".format(dfOriolesNotPlaying.shape[0]))




