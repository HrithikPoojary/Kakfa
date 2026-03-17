To Create Topics

>bin/kafka-topics.sh \
>--create \
>--topic <topic_name> \
>--bootstrap-server localhost:9092 \
>--partitions <number_of_partitions (0<) \
>--replication-factor <number of replications>

To list the Topics

>bin/kafka-topics.sh \
>--list \
>--bootstrap-server localhost:9092


To check topics in folders

go to root directory cd ~/../..

>cd tmp/kafka-logs
