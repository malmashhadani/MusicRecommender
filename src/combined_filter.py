from pyspark import SparkContext, SparkConf
import numpy as np
import scipy.spatial.distance as ssd

def songSplit(x):
	x = x.split("\t")
	return tuple((tuple([str(x[0])]),tuple([float(x[1]),float(x[2])])))

def userSplit(x):
	x = x.split("\t")
	return tuple((tuple([str(x[1])]),tuple([str(x[0]),float(x[2])])))
def joinFlip(x):
	x = list(x)
	r = list(x[1])
	user_rating = list(r[0])
	feature = list(r[1])
	if user_rating[1] > 0:
		user_pref = [list(x[0])[0],user_rating[1],feature[0],feature[1]]
	else:
		user_pref = [list(x[0])[0],None,feature[0],feature[1]]
	return tuple((tuple([user_rating[0]]),tuple([user_pref])))
def keys(x,y):
	return tuple(x+y)


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



def hoping(x):
	line = list(x)
	user = list(x[0])[0]
	items = [(user,line[1][0],line[1][1])]
	i=2
	while i<len(line):
		items.append((user,line[i][0],line[i][1]))
		i+=1
	print user
	return items


def main():
	conf = SparkConf().setAppName("mm").set("spark.executor.memory", "2g")
	sc = SparkContext(conf=conf)
	RDDplayed = sc.textFile('train_visibleSmall.txt')
	songs = sc.textFile('Song_PropertiesSmall.txt')
	RDDnot_played = sc.textFile('notplayedsongs.txt')
	features = songs.map(lambda x: songSplit(x))
	played_flipped = RDDplayed.map(lambda x: userSplit(x))
	played_joined = played_flipped.join(features)
	flip_played_joined = played_joined.map(lambda x: joinFlip(x))
	rated = flip_played_joined.reduceByKey(lambda x,y:keys(list(x),list(y)))
	

	notplayed_flipped = RDDnot_played.map(lambda x: userSplit(x))
	notplayed_joined = notplayed_flipped.join(features)
	flip_notplayed_joined = notplayed_joined.map(lambda x: joinFlip(x))
	unrated = flip_notplayed_joined.reduceByKey(lambda x,y:keys(list(x),list(y)))
	


	joined_RDD = rated.join(unrated)
	

	rates = joined_RDD.map(lambda x:comparison_function(x))

	hope = rates.flatMap(hoping)
	hope_for_better = hope.map(lambda x: str(x[0])+"\t"+str(x[1])+"\t"+str(x[2]))
	print hope_for_better.collect()
	hope_for_better.coalesce(1).saveAsTextFile('thehope')



if __name__ == '__main__':
	main()