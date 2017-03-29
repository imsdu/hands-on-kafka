docker run --rm --net=host -v ~/projects/devoxx-handson-java:/data landoop/fast-data-dev

name=scala-gitlog
connector.class=org.apache.kafka.connect.file.FileStreamSourceConnector
tasks.max=1
file=/data/scala-gitlog.json
topic=scala-gitlog
value.converter=org.apache.kafka.connect.storage.StringConverter
key.converter=org.apache.kafka.connect.storage.StringConverter

for i in {1..11}; do ( http -a user:pass --pretty=none https://api.github.com/repos/scala/scala/contributors\?page\=$i --pretty=none | jq --compact-output '.[]' ) >> /tmp/contributors.json done
for i in {1..901}; do ( http -a user:pass --pretty=none https://api.github.com/repos/scala/scala/commits\?page\=$i --pretty=none | jq --compact-output '.[]' ) >> ./commits.json done
