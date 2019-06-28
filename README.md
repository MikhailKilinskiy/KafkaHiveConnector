# Description
testing_afosp - command-line utils for running ETL process between Hive/HDFS and Kafka. Data must be in Avro format and support Confluent schema registry.
###Usage example
    #!/usr/bin/env bash

    export SPARK_MAJOR_VERSION=2


    spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --files "jaas.conf,application.conf,keytab" \
    --conf "spark.executor.extraJavaOptions=- Djava.security.auth.login.config=jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
    --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=jaas.conf -Djava.security.krb5.conf=/etc/krb5.conf" \
    --conf "spark.yarn.access.hadoopFileSystems=***" \
    KafkaHiveConnector-spark.jar --job Kafka2RemoteHDFS

## Parametrs and functionality (--job)

- Hive2Kafka - transfer data from Hive to Kafka.
- Kafka2Hive - transfer data from Kafka to Hive.
- Kafka2HDFS -  transfer data from Kafka to Hdfs.
- Kafka2RemoteHDFS - transfer data from Kafka to Hdfs at the remote server. Need to use spark.yarn.access.hadoopFileSystems in spark-submit.