```
./cde resource create \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-python \
  --type python-env \
  --python-version python3
  
cat requirements.txt
boto3
impyla

./cde resource upload \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-python \
  --local-path /Users/kdavis/Documents/Demos/CDE/requirements.txt

./cde resource list-events \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-python

./cde --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 resource create --name demo-resource-py

./cde --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 resource upload --name demo-resource-py --local-path AppETLCrimeData.py

./cde job create \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --type spark \
  --application-file AppETLCrimeData.py \
  --python-env-resource-name demo-resource-python \
  --name demo-resource-python \
  --mount-1-resource demo-resource-py

./cde job create \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --type spark \
  --application-file AppETLCrimeData.py \
  --python-env-resource-name demo-resource-python \
  --name demo-resource-python \
  --mount-1-resource demo-resource-py

./cde job run \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-python

./cde run \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  describe --id 7 | jq -r '.status'
  
```
