```
% sudo wget https://services.gradle.org/distributions/gradle-5.0-bin.zip -P /tmp
% sudo unzip -d /opt/gradle /tmp/gradle-5.0-bin.zip

% sudo wget http://downloads.lightbend.com/scala/2.12.14/scala-2.12.14.rpm
% sudo yum install scala-2.12.14.rpm

% sudo cp /etc/profile /etc/profile_backup
% echo 'export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk' | sudo tee -a /etc/profile
% echo 'export JRE_HOME=/usr/lib/jvm/jre' | sudo tee -a /etc/profile
% echo 'export SCALA_HOME=/usr/share/scala/' | sudo tee -a /etc/profile
% echo 'export SPARK_HOME=/home/ec2-user/spark-2.4.7-bin-without-hadoop-scala-2.12' | sudo tee -a /etc/profile
% echo 'export GRADLE_HOME=/opt/gradle/gradle-5.0' | sudo tee -a /etc/profile
% echo 'export PATH=${PATH}:${SCALA_HOME}/bin:${SPARK_HOME}/bin:${GRADLE_HOME}/bin' | sudo tee -a /etc/profile
% source /etc/profile

% mkdir ~/Development/demo
% cd ~/Development/demo
% gradle init

% cat build.gradle

plugins {
  id 'scala'
  id 'application'
}

repositories {
  mavenCentral()
}

dependencies {
  compile 'org.scala-lang:scala-library:2.11'
  implementation 'org.scala-lang:scala-library:2.11'
// Spark SQL subsumes Spark Core
  implementation 'org.apache.spark:spark-sql_2.11:2.4.7'
// Kudu dependencies
  implementation group: 'org.apache.kudu', name: 'kudu-spark2_2.11', version: '1.13.0'
}

application {
  mainClassName = 'demo.AppPopulateKudu'
}
```
