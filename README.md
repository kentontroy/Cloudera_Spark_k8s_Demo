## General Pattern <br>

<img src="./parquet_kudu_hbase.png" alt="Simple pattern"/> <br>
<ul>
<li>An Akka typed Actor system is used with a Guardian to control the lifecycle of a JMS Consumer</li>
<li>The JMS Consumer reads messages from an ActiveMQ instance running within a Docker container</li>
<li>Configuration options are set to control the session count, batch size, Ack timeout, and polling interval</li>
<li>Akka stream types are used to wrap the JMS message consumed within a transaction envelope</li>
<li>If the JMS message is successfully written to S3 (via multi-part upload), the transaction is committed</li>
</ul>
