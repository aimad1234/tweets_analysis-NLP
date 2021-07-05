   
# Code pour l’analyse en continu des tweets avec Spark Streaming
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
import json




# Configuration de Spark Streaming toutes les 5 secondes
conf = SparkConf().setMaster("local").setAppName("AnalyseTweets")
sc = SparkContext.getOrCreate(conf=conf)
stream_nbseconds=5
ssc = StreamingContext.getActiveOrCreate(sc, stream_nbseconds)
# Connexion au topic Kafka dédié au streaming des tweets
topic="quickstart-events"
kafkaStream = KafkaUtils.createDirectStream(ssc,[topic],{"metadata.broker.list":"LAPTOP-GL13JIPM:9092"})
parsed = kafkaStream.map(lambda v: json.loads(v[1]))

counts = parsed.flatMap(lambda line: line.split(" "))\
                    .filter(lambda word:"ERROR" in word)\
                    .map(lambda word : (word, 1))\
                    .reduceByKey(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y))

        ## Display the counts
        ## Start the program
        ## The program will run until manual termination
counts.pprint()
ssc.start()
ssc.awaitTermination()


'''
# Analyse en continu des hashtags dans les tweets
words = parsed.flatMap(lambda line: line.replace('"',' ').replace("'",' ').replace
("(",' ').replace(")",' ').replace("\\",' ').replace(".",' ').split())
hashtags=words.filter(lambda w: re.findall(r'\B#\w*[a-zA-Z]+\w*',w)).map(lambda x:
(x, 1))
# fenêtre d'analyse des tweets toutes les 20 secondes
stream_window_nbseconds=20
stream_slide_nbseconds=20
lambda x, y: x - y, stream_window_nbseconds,stream_slide_nbseconds
hashtags_reduced = hashtags.reduceByKeyAndWindow(lambda x, y: x + y,lambda x, y: int(x) - int(y), 10, 2)
hashtags_sorted = hashtags_reduced.transform(lambda w: w.sortBy(lambda x: x[1],
ascending=False))
# Affichage et sauvegarde des résultats
hashtags_sorted.pprint()
#hashtags_sorted.foreachRDD(lambda rdd: rdd.foreach(insertHashtags))
# demarrage de la boucle de traitement des tweets
ssc.start()'''

