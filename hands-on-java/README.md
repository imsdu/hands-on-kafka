# Hands-on Kafka Streams - Devoxx 2017

## Install

- Extract m2 archive in your local maven repository
- Extract sources in a directory and import it in your favorite IDE

Before All you have to adapt the file fr.devoxx.kafka.conf.AppConfiguration depend of your setup.

Principally the value `URL_BASE :

- if you are in linux  user `localhost`
- if you use some VM put the address of it

## Running landoop docker and launch all kafka services on linux
```
docker run --rm --net=host -v path/to/project/hands-on-kafka/data:/data -e FORWARDLOGS=0 -e RUNTESTS=0 -e DEBUG=1 landoop/fast-data-dev
```

You may have to make file access for containers more permissive via the command:
```
su -c "setenforce 0"
```

## Run landoop/fast-data-dev on mac

On Mac OS X allocate at least 6GB RAM to the VM:

    docker-machine create --driver virtualbox --virtualbox-cpu-count "4"  --virtualbox-memory "6024" devel
    eval $(docker-machine env devel)


Execute the result of

```
docker-machine env devel
```

And define ports and advertise hostname:
 <!>  Replace  `myPathToTheProject` with a real path <!>

```
docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
           -p 9581-9585:9581-9585 -p 9092:9092 -v /myPathToTheProject/hands-on-kafka/data:/data -e ADV_HOST=192.168.99.100  -e CONNECT_HEAP=3G -e FORWARDLOGS=0 -e RUNTESTS=0 -e DEBUG=1 \
           landoop/fast-data-dev:latest

```

## Open shell on running container
```
docker ps
```

```
docker exec -it [containerId] sh
```

## Init kafka topics
```
init.[sh|bat]
```

## Reset application id
In your container shell, execute :
```
kafka-streams-application-reset  --bootstrap-servers localhost:9092 --zookeeper localhost:2181 --input-topics [topic] --application-id [my-app]
```

## Profit with some exercises !

### Kafka Connect
- Import data from the file scala-git.json from a file source connector to a kafka topic scala-gitlog-connect and then export it via a file sink connector to another file using StringConverters
- Add new lines to input file

### Stateless transformations
- CommitContributor: Create a topic with [key, value] =>  [ID COMMIT,CONTRIBUTOR]
- FixStreams / CommitComment: Create a topics related about a fix and another for the others [ID COMMIT,COMMENT]
- BranchCommit: Create 2 topic with [key, value] =  [ID ,COMMIT] with commit relate to a fix and not
- ContributorNoLightbend: List of commit from  contributor who doesn't work for Lightbend
- ContributorsStreams: Create a topic with [key, value] =  [ID CONTRIBUTOR,CONTRIBUTOR INFO]

### Stateful transformations
- CountNbrCommitByUser: With a table with  a count of commit by contributor
- FixCommit: Make a table for know how many commit relate to a "fix" for each year
- TotalCommitMessageByUser: Make a table with the total size of comment by author
- NbrCommitByContributorCategory: Make a table with the total of commit from people who work for EPFL, Lightbend and Other

### Joining
- StreamToTableJoin: Join user table with commit stream to find users who have commited on the Scala repository
- TableToTableJoin: Join user table with commit table to get the last commit on Scala repository for each user
- StreamToStreamJoin: Join the user with streams with commit streams to find number of commits for each user (even for users who haven't commited on Scala repository)

### Windowing
### Querying local key-value stores
- LocalKVStore: Count Number of commits per user 
### Querying local window stores
- WindowedLocalKVStore: Count Number of commits per user over windowed periods

### Exposing the REST endpoints of your application
- Exposing  state off Stateful exercices :  RUN InteractiveQueries with argumente `localhost:9000 192.168.99.100:9092` for mac or `localhost:9000 localhost:9092` for linux
- Exposing top 10 contributors  (Confluent API)
- Use you favorite RPC/REST library/framework to expose !
