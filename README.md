### Reasons to use Kudu <br>

<img src="./parquet-kudu-hbase.png" alt="Kudu positioning"/><br>
<ul>
<li>Distributed, columnar storage engine outside of HDFS</li>
<li>Intuitive organization of data on disk</li>
<li>SQL Access Layer via Impala</li>
<li>Speed in both bulk and for random access reads and writes</li>
<li>Accommodation of upserts</li>
</ul>

<img src="./kudu-architecture.png" alt="Kudu architecture"/><br>

Tables uses partitions spread across Tablets. Each Tablet uses leader and follower nodes with replication <br>
to ensure resiliency. Data stored in a columnar fashion enables sequential storage, vectorization and compression. <br>



