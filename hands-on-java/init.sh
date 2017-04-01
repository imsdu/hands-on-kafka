mvn exec:java -Dexec.mainClass="fr.devoxx.kafka.producer.InitProducer" -Dexec.args="../data/users.json users"
mvn exec:java -Dexec.mainClass="fr.devoxx.kafka.producer.InitProducer" -Dexec.args="../data/scala-gitlog.json scala-gitlog"
mvn exec:java -Dexec.mainClass="fr.devoxx.kafka.producer.InitProducer" -Dexec.args="../data/contributors.json contributors"
mvn exec:java -Dexec.mainClass="fr.devoxx.kafka.producer.InitProducer" -Dexec.args="../data/all-users.json all-users"
