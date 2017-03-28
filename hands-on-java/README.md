docker run --rm --net=host -v ~/projects/devoxx-handson-java:/data landoop/fast-data-dev

name=scala-gitlog
connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector
tasks.max=1
file=/data/scala-gitlog.json
topic=scala-gitlog
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter=org.apache.kafka.connect.storage.StringConverter