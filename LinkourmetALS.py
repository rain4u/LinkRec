import sys
# import itertools
# from math import sqrt
# from operator import add
# from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def init():
    conf = SparkConf().setAppName("LinkourmetALS")
    return SparkContext(conf=conf)


def loadData(sc):
    # currently load from a file, TODO: load from database
    return sc.textFile("ratings.dat").map(lambda l: l.split('::')).map(lambda l: (int(l[0]), int(l[1]), float(l[2])))


def getUserData(data, userID):
    sharedLinks = data.filter(lambda r: r[0] == userID).map(lambda r: r[1])
    allLinks = data.map(lambda r: r[1]).distinct()

    unsharedLinks = allLinks.subtract(sharedLinks)
    return unsharedLinks.map(lambda x: (userID, x))


def findBestModel(data):
    # Build the recommendation model using Alternating Least Squares
    # TODO need to test the best configuration of model
    rank = 10
    numIterations = 20
    return ALS.trainImplicit(data, 1)


def recommend(model, userData):
    predictions = model.predictAll(userData).map(lambda x: (x[0], x[1], x[2])).collect()
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)
    return recommendations


if __name__ == "__main__":
    
    sc = init()

    if len(sys.argv) != 2:
        sys.exit('Usage: [spark root]/bin/spark-submit %s {userID}' % sys.argv[0])
    
    userID = int(sys.argv[1])

    data = loadData(sc) # data is RDD of (user, link, rating)
    print('[XC] All Data:')
    print(data.collect());

    model = findBestModel(data)

    userData = getUserData(data, userID) # userData is RDD of (user, link) where 'link' is not shared by this user
    print('[XC] User Data:')
    print(userData.collect());

    reclinks = recommend(model, userData)
    print('[XC] Recommend links:')
    for linkInfo in reclinks:
        print (linkInfo[1])

    sc.stop()