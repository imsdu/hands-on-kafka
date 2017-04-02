#Configuration

Before All you have to adapt the file fr.devoxx.kafka.conf.AppConfiguration depend of your setup.

Principally the value `URL_BASE :

- if you are in linux  user `localhost`
- if you use some VM put the address of it



#Exos

## Stateless transformations - Cecilia
- Create a topic with [key, value] =>  [ID COMMIT,CONTRIBUTOR]
- Create a topics related about a fix and another for the others [ID COMMIT,COMMENT]
- Create a topic with [key, value] =  [ID CONTRIBUTOR,CONTRIBUTOR INFO]
- Create 2 topic with [key, value] =  [ID ,COMMIT] with commit relate to a fix and not
- Create POJO pour user and a topic with [key, value] =  [ID USER,USER]
- List of commit from  contributor who doesn't work for Lightbend


## Stateful transformations- Cecilia
- With a table with with a count of commit by contributor
- Make a table for know how many commit relate to a "fix" for each year
- Make a table with the total size of comment by author
- Make a table with the total of commit from people who work for EPFL, Lightbend and Other

## Joining -  Dumas
## Windowing - Saleh
## Querying local key-value stores -  Saleh
## Querying local window stores -  Saleh

## Exposing the REST endpoints of your application - Cecilia
- Exposing  data from Stateful exercices
- Exposing top 10 contributors  (Confluent API)
- Use you favorite RPC/REST library/framework to expose !
