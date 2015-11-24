from pyspark import SparkContext, SparkConf
import numpy as np
import scipy.spatial.distance as ssd



def comparison_function(x):

	# Extract lists from the tuple inputs

	# each list represent a user list of songs
	x = list(x)
	songs = list(x[1])
	rated_songs = list(songs[0])
	unrated_songs = list(songs[1])
	i=0

	# loop through not played songs
	for unrated_song in unrated_songs:
		feature_unrated = np.array((float(unrated_song[2]),float(unrated_song[3])))
		closest = 1

		# loop through played songs
		for rated_song in rated_songs:
			feature_rated = np.array((float(rated_song[2]),float(rated_song[3])))
			dist = ssd.cosine(feature_rated,feature_unrated)
			if dist<closest:
				closest=dist
				rate = rated_song[1]
		unrated_songs[i][1] = rate
		i+=1
	final_rated = rated_songs+unrated_songs
	final_rated = tuple(final_rated)
	final = tuple([x[0]]) + final_rated
	return tuple(final)



def main():

	# RDD of played songs
	rated = (('u1',(['s1', 7,0.5, 0.6],['s2', 2,0.1, 0.2],['s4', 3,0.9, 0.8])), ('u2',(['s3', 4,0.5, 0.6],['s5', 9,0.1, 0.2])))
	conf = SparkConf().setAppName("mm").set("spark.executor.memory", "300mb")
	sc = SparkContext(conf=conf)
	RDDp = sc.parallelize(rated)

	# RDD of not played songs

	unrated = (('u1',(['s3', None,0.5,0.6],['s5',None,0.1,0.2])), ('u2',(['s1', None,0.5,0.7],['s2',None,0.1,0.2],\
		['s4',None, 0.9, 0.8])))
	RDDnp = sc.parallelize(unrated)
	joined_RDD = RDDp.join(RDDnp)

	
	rates = joined_RDD.map(lambda x:comparison_function(x))
	print rates.take(3)
	

if __name__ == '__main__':
	main()