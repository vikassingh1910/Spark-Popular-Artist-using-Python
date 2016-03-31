from pyspark import SparkConf, SparkContext

def loadArtistNames():
    artistNames = {}
    with open("hetrec2011-lastfm-2k/artists.dat") as f:
        for line in f:
            fields = line.split('\t')
            artistNames[int(fields[0])] = fields[1]
    return artistNames

conf = SparkConf().setMaster("local").setAppName("PopularArtistByName")
sc = SparkContext(conf = conf)

# use of broadcast variable
artistNames = sc.broadcast(loadArtistNames())

# loading the userid and artist id
lines = sc.textFile("file:///spark/hetrec2011-lastfm-2k/user_taggedartists-timestamps.dat")
artist = lines.map(lambda x: (int(x.split()[1]), 1))
artistCounts = artist.reduceByKey(lambda x, y: x + y)

flipped = artistCounts.map( lambda (x, y) : (y, x))
sortedArtist = flipped.sortByKey()

#searching the artist name from artists dictionary
sortedArtistWithNames = sortedArtist.map(lambda (count, artist) : (artistNames.value[artist], count))

results = sortedArtistWithNames.collect()

for result in results:
    print result


