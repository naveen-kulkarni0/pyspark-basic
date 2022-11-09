import findspark
findspark.init()

import pyspark
sc = pyspark.SparkContext(appName="maps_and_lazy_evaluation_example")

log_of_songs = [
    "Despacito",
    "Nice for what",
    "No tears",
    "Who let the dogs out"
]

distributed_song_log = sc.parallelize(log_of_songs)

intermittent_rdd = distributed_song_log.map(lambda x: x.lower())
songs_in_lower = intermittent_rdd.collect()
song_count = intermittent_rdd.count()

print(songs_in_lower)
print(song_count)