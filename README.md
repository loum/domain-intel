# Domain Intel Services

## Getting Started ##
### Building the Development Environment ###
Use the project `Vagrant` file to build  a localised development
environment:
```
$ vagrant up
```

Once complete, `ssh` into the machine:
```
$ vagrant ssh
vagrant@domain-intel:~$
```

## Development ##
From the vagrant host prompt, `sudo` as the user `domainintel.

```
vagrant@domain-intel:~$ sudo su - domainintel
domainintel@domain-intel:~$
```

Domain Intel source distribution supports universal wheels meaning that it
can run under both Python 2 and 3 and can be installed without needing to
go through the standard build process.

The Vagrant setup process will build working environments for both `python2`
and `python3`.  However, it is safe to remove these and rebuild at any time
using the project's `Makefile`:

```
domainintel@domain-intel:~$ cd domain-intel/
domainintel@domain-intel:~/domain-intel$ make init PYVERSION=2 APP_ENV=development
...
domainintel@domain-intel:~/domain-intel$ make init PYVERSION=3 APP_ENV=development
...
```

Moving between virutal environments can be achieved sourcing the appropriate
`activate`:

```
domainintel@domain-intel:~/domain-intel$ source 2env/bin/activate
(2env) domainintel@domain-intel:~/domain-intel$ source 3env/bin/activate
(3env) domainintel@domain-intel:~/domain-intel$
```

### Running the Tests ###
From with a virtual environment, the test can be run with `make test`:

```
(3env) domainintel@domain-intel:~/domain-intel$ make test
```

If using a Mac, you can also run the tests directly from you desktop.  However, you will need to attach an unused IP to the `lo0` interface.  This is a known limitation.  The command to use is:

```
$ sudo ifconfig lo0 alias 10.200.10.1/24
```

### Adding a New Topic to Kafka ###
Unfortunately, this has to be managed in two places for test and production.

#### Test Environment Kafka ####
Locate the `kafka["topics"]` key in the `config/dev.json` file and add your new topic to the list.
For example:

**old:**
```
{
    ...
    "kafka": {
        "topics": "gtr-domains:5:1,alexa-results:5:1",
        "port": 9091,
    ...
}
```

**new:**
```
{
    ...
    "kafka": {
        "topics": "gtr-domains:5:1,alexa-results:5:1,new-whiz-bang:1:1",
        "port": 9091,
    ...
}
```

This will ensure that the Kafka instance will build with your new topics in place.

#### Production Environment Kafka ####
The Domain Intel configuration for production is managed in Ansible.  Add your new topic name to the
`topics` global variable in `group_vars/app-domain-intel-services`.

## Managing the Kafka Instance ##
The Kafka instance is based on [this docker image](https://github.com/wurstmeister/kafka-docker).

### Docker Kafka Management on the Vagrant Guest ###
`ssh` into the Vagrant guest:
```
vagrant ssh
```

The Kafka docker image can be found at `/docker/kafka`.

To start Kafka as a daemon:
```
(3env) domainintel@domain-intel:~$ cd /docker/kafka/
(3env) domainintel@domain-intel:/docker/kafka$ docker-compose -f docker-compose-single-broker.yml up -d
```

To stop:
```
$ docker-compose -f docker-compose-single-broker.yml down
```

### Testing Kafka ###
You can interact with the Kafka cluster via the Kafka shell:
```
$ cd /docker/kafka
$ ./start-kafka-shell.sh <DOCKER_HOST_IP> <ZK_HOST:ZK_PORT>
```

**Note** the <DOCKER_HOST_IP> and <ZK_HOST:ZK_PORT> details can be taken from the `docker-compose-single-broker.yml` file's `KAFKA_ADVERTISED_HOST_NAME` and `KAFKA_ZOOKEEPER_CONNECT` variables.  For example:

```
$ ./start-kafka-shell.sh 10.200.10.1 10.200.10.1:2181
bash-4.3#
```

Start a producer on a newly created topic:
```
bash-4.3# $KAFKA_HOME/bin/kafka-topics.sh --create --topic topic --partitions 5 --zookeeper $ZK --replication-factor 1
bash-4.3# $KAFKA_HOME/bin/kafka-topics.sh --describe --topic topic --zookeeper $ZK
Topic:topic    PartitionCount:5    ReplicationFactor:1    Configs:
    Topic: topic    Partition: 0    Leader: 1001    Replicas: 1001    Isr: 1001
    Topic: topic    Partition: 1    Leader: 1001    Replicas: 1001    Isr: 1001
    Topic: topic    Partition: 2    Leader: 1001    Replicas: 1001    Isr: 1001
    Topic: topic    Partition: 3    Leader: 1001    Replicas: 1001    Isr: 1001
    Topic: topic    Partition: 4    Leader: 1001    Replicas: 1001    Isr: 1001
bash-4.3# $KAFKA_HOME/bin/kafka-console-producer.sh --topic=topic --broker-list=`broker-list.sh`
```
This will block the CLI.

Now, start another shell and start a consumer:
```
sudo ./start-kafka-shell.sh 10.200.10.1 10.200.10.1:2181
bash-4.3# $KAFKA_HOME/bin/kafka-console-consumer.sh --topic=topic --zookeeper=$ZK
```

In the producer session type a message.  This should be displayed in the consumer shell

To end the producer and consumer hit Ctrl-C in both sessions.

### Useful Kafka Commands ###

#### List all topics ####
```
bash-4.3# /opt/kafka/bin/kafka-topics.sh --zookeeper $ZK --list
alexa-flattened
alexa-results
gtr-domains
```

#### List the number of messages in a topic's partions ####
```
bash-4.3# /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 10.200.10.1:9092 --topic gtr-domains --time -1
gtr-domains:2:0
gtr-domains:4:0
gtr-domains:1:0
gtr-domains:3:0
gtr-domains:0:0
```

With a summing technique:
```
bash-4.3# /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list 10.200.10.1:9092 --topic gtr-domains --time -1 | awk -F ':' '{sum += $3} END {print sum}'
0
```

## Persistent Store - ArangoDB ##
The ArangoDB docker image can be found at `/docker/arangodb`.

To start ArangoDB as a daemon:
```
(3env) domainintel@domain-intel:~$ cd /docker/arangodb/
(3env) domainintel@domain-intel:/docker/arangodb$ docker-compose up -d
```

To stop:
```
(3env) domainintel@domain-intel:/docker/arangodb$ docker-compose down
```

The ArangoDB UI can be accessed from http://localhost:8529/
