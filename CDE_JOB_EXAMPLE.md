### Traditional method
```
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf "spark.workflow.name=CalculatePrimes" \
  --driver-memory 10g \
  --executor-memory 2g \
  --executor-cores 5  \
  --num-executors 15 \
  --jars /tmp/UnravelTesting.jar \
  --class com.cloudera.pse.demo.primes.CalculatePrimes \
  SkillUp_Calculate_Primes.jar 10000
```
### CDE CLI
```
./cde --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  spark submit SkillUp_Calculate_Primes.jar \
  --class com.cloudera.pse.demo.primes.CalculatePrimes \
  SkillUp_Calculate_Primes.jar 10000
````
### CDE Job
````
./cde --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 resource create --name demo-resource

./cde --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 resource upload --name demo-resource --local-path SkillUp_Calculate_Primes.jar

./cde job create \
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-primes \
  --type spark \
  --mount-1-resource demo-resource \
  --application-file SkillUp_Calculate_Primes.jar 

./cde job run \ 
  --vcluster-endpoint=https://72s24ncc.cde-n44bzzmp.se-sandb.a465-9q4k.cloudera.site/dex/api/v1 \
  --name demo-resource-primes \
  --arg 10000
{
```
