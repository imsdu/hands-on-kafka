# Hands-on Kafka Streams - Devoxx 2017

## Install

- Extract m2 archive in your local maven repository
- Extract sources in a directory and import it in your favorite IDE

Before All you have to adapt the file fr.devoxx.kafka.conf.AppConfiguration depend of your setup.

Principally the value `URL_BASE :

- if you are in linux  user `localhost`
- if you use some VM put the address of it

## Running landoop docker and launch all kafka services
```
docker run --rm --net=host -v /home/sdumas/projects/hands-on-kafka/data:/data -e FORWARDLOGS=0 landoop/fast-data-dev
```

## Init kafka topics
```
init.[sh|bat]
```

## Profit with some exercises !

### Kafka Connect
- Import data from the file scala-git.json from a file source connector to a kafka topic scala-gitlog-connect and then export it via a file sink connector to another file using StringConverters
- Add new lines to input file

### Stateless transformations
- Create a topic with [key, value] =>  [ID COMMIT,CONTRIBUTOR]
- Create a topics related about a fix and another for the others [ID COMMIT,COMMENT]
- Create a topic with [key, value] =  [ID CONTRIBUTOR,CONTRIBUTOR INFO]
- Create 2 topic with [key, value] =  [ID ,COMMIT] with commit relate to a fix and not
- Create POJO pour user and a topic with [key, value] =  [ID USER,USER]
- List of commit from  contributor who doesn't work for Lightbend

### Stateful transformations
- With a table with with a count of commit by contributor
- Make a table for know how many commit relate to a "fix" for each year
- Make a table with the total size of comment by author
- Make a table with the total of commit from people who work for EPFL, Lightbend and Other

### Joining
- Join user table with commit stream to find users who have commited on the Scala repository
- Join user table with commit table to get the last commit on Scala repository for each user
- Join the user with streams with commit streams to find number of commits for each user (even for users who haven't commited on Scala repository)

### Windowing
### Querying local key-value stores
- Count Number of commits per user 
### Querying local window stores
- Count Number of commits per user over windowed periods

### Exposing the REST endpoints of your application
- Exposing  state off Stateful exercices :  RUN InteractiveQueries with argumente `localhost:9000 192.168.99.100:9092` for mac or `localhost:9000 localhost:9092` for linux
- Exposing top 10 contributors  (Confluent API)
- Use you favorite RPC/REST library/framework to expose !
