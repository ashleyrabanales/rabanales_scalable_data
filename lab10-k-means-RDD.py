from pyspark.context import SparkContext, SparkConf


# Started Spark Context with Spark Session 
conf = SparkConf().setMaster("local").setAppName("kMeansNoMLlib")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


# Read in the txt file
read_data = sc.textFile('kmeans_data.txt')
# Split each line into a list seperated by " "
read_data = read_data.map(lambda x: x.split(" "))

"""
Without MLlib Library
"""
import math 
import numpy as np

def euc_dist(p1, p2):
    # takes in two lists p1 and p2 and calculate euclidean distance
    # assumes that there are only 2 elements in list 
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2 )

def find_cluster(samp, points):
    # Takes in a list, sample points, and a list with 2 lists within it, the cluster centers. 
    # This will return which cluster the sample points is closest to
    if min(euc_dist(samp, points[0]) ,euc_dist(samp, points[1]))== euc_dist(samp, points[0]):
        # returns 1 if the sample points are closer to the first center
        return 1
    else: 
        # returns 2 if the sample points are closer to the second center
        return 2
def dist_to_cluster(cluster, samp, points): 
    # given the cluster, calucluates the exact distance to the center given a point
    if cluster == 1:
        # gives the distance to cluster 1
        return euc_dist(samp, points[0])
    else:
        # gives the distance to cluster 2
        return euc_dist(samp, points[1])

# Converts each line into a numpy array containing just the X and Y columns
data_nolib = read_data.map(lambda x: np.array([float(x[1]), float(x[2])]))
# Sets random points sampled from our dataset 
cluster_center = data_nolib.takeSample(False, 2)
# set a intial point so that our loop keeps going until convergence onto error
dist = 100
Count = 1

print("\n")
print("Initial points and randomly generated cluster centers")
print(data_nolib.collect())
print(cluster_center)
print("\n")

while dist > 1:
    # decided that once the total sum of distances from the corresponding cluster
    # reached < 1, this will stop

    # Map step
    print("1. Map stage,label each node with cluster center")
    # from the data, i created a key value pair: (cluster, (points, 1))
    cluster = data_nolib.map(lambda line: 
        (find_cluster(line, cluster_center), (line,1))
    )
    print(cluster.collect())
    print("\n")

    print("2. Reduce(ByKey) stage,find the new cluster center")
    # we will reduce by cluster and add (points,1) to each other
    # since points are a numpy array, it will add element by element 
    # ie np.array([1,2]) + np.array([2,3]) = np.array([3,5])
    # whereas if it were a list 
    # [1,2] + [2,3] = [1,2,2,3]
    # our output would now just be (cluster, (sum_of_points, sum_of_counter))
    cluster = cluster.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
    print(cluster.collect())
    # We now take the average, now these will be used as the new cluster centers, 
    # output would be (cluster, avg_of_points)
    cluster = cluster.mapValues(lambda x: x[0]/x[1])
    print(cluster.collect())
    cluster_center = cluster.map(lambda x: x[1]).collect()
    print(cluster_center)
    print("\n")

    print("2. Reduce stage, obtain the clustering error")
    # We need to calculate distances as well to stop the loop 
    # So I made the key value pair now (cluster, points)
    dist = data_nolib.map(lambda line: 
        (find_cluster(line, cluster_center), line)
    )
    print(dist.collect())
    # Will just map it to [cluster, distance_to_cluster_assigned]
    dist = dist.map(lambda x: (x[0], dist_to_cluster(x[0],x[1],cluster_center)))
    print(dist.collect())
    # Then i reduced by cluster and added the distance, grabbed only the distance, then added them together 
    dist = dist.map(lambda x: x[1])
    print(dist.collect())
    dist = dist.reduce(lambda x,y:x+y)
    # set new cluster center by grabbing the average points
    print(dist)
    print(str(Count)+ " round\n\n")
    Count = Count + 1
    print("\n")

print("Cluster centers:")
for center in cluster_center:
    print(center)
