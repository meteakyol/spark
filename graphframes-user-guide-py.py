# Databricks notebook source
from functools import reduce
from pyspark.sql.functions import col, lit, when
from graphframes import *

# COMMAND ----------

# COMMAND ----------

vertices = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)], ["id", "name", "age"])

# COMMAND ----------

# MAGIC %md And then some edges:

# COMMAND ----------

edges = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

# COMMAND ----------

# MAGIC %md Let's create a graph from these vertices and these edges:

# COMMAND ----------

g = GraphFrame(vertices, edges)
print(g)

# COMMAND ----------

# This example graph also comes with the GraphFrames package.
from graphframes.examples import Graphs
same_g = Graphs(sqlContext).friends()
print(same_g)

# COMMAND ----------

display(g.vertices)

# COMMAND ----------

display(g.edges)

# COMMAND ----------

# MAGIC %md The incoming degree of the vertices:

# COMMAND ----------

display(g.inDegrees)

# COMMAND ----------

# MAGIC %md The outgoing degree of the vertices:

# COMMAND ----------

display(g.outDegrees)

# COMMAND ----------

# MAGIC %md The degree of the vertices:

# COMMAND ----------

display(g.degrees)

# COMMAND ----------

# MAGIC %md You can run queries directly on the vertices DataFrame. For example, we can find the age of the youngest person in the graph:

# COMMAND ----------

youngest = g.vertices.groupBy().min("age")
display(youngest)

# COMMAND ----------

# MAGIC %md Likewise, you can run queries on the edges DataFrame. For example, let's count the number of 'follow' relationships in the graph:

# COMMAND ----------

numFollows = g.edges.filter("relationship = 'follow'").count()
print("The number of follow edges is", numFollows)

# COMMAND ----------

# Search for pairs of vertices with edges in both directions between them.
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
display(motifs)

# COMMAND ----------

# MAGIC %md Since the result is a DataFrame, more complex queries can be built on top of the motif. Let us find all the reciprocal relationships in which one person is older than 30:

# COMMAND ----------

filtered = motifs.filter("b.age > 30 or a.age > 30")
display(filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stateful queries
# MAGIC
# MAGIC Most motif queries are stateless and simple to express, as in the examples above. The next example demonstrates a more complex query that carries state along a path in the motif. Such queries can be expressed by combining GraphFrame motif finding with filters on the result where the filters use sequence operations to operate over DataFrame columns.
# MAGIC
# MAGIC For example, suppose you want to identify a chain of 4 vertices with some property defined by a sequence of functions. That is, among chains of 4 vertices `a->b->c->d`, identify the subset of chains matching this complex filter:
# MAGIC
# MAGIC * Initialize state on path.
# MAGIC * Update state based on vertex a.
# MAGIC * Update state based on vertex b.
# MAGIC * Etc. for c and d.
# MAGIC
# MAGIC If final state matches some condition, then the filter accepts the chain.
# MAGIC The below code snippets demonstrate this process, where we identify chains of 4 vertices such that at least 2 of the 3 edges are “friend” relationships. In this example, the state is the current count of “friend” edges; in general, it could be any DataFrame Column.

# COMMAND ----------

# Find chains of 4 vertices.
chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

# Query on sequence, with state (cnt)
#  (a) Define method for updating state given the next element of the motif.
def cumFriends(cnt, edge):
  relationship = col(edge)["relationship"]
  return when(relationship == "friend", cnt + 1).otherwise(cnt)

#  (b) Use sequence operation to apply method to sequence of elements in motif.
#   In this case, the elements are the 3 edges.
edges = ["ab", "bc", "cd"]
numFriends = reduce(cumFriends, edges, lit(0))
    
chainWith2Friends2 = chain4.withColumn("num_friends", numFriends).where(numFriends >= 2)
display(chainWith2Friends2)

# COMMAND ----------

# MAGIC %md ## Subgraphs
# MAGIC
# MAGIC GraphFrames provides APIs for building subgraphs by filtering on edges and vertices. These filters can be composed together, for example the following subgraph only includes people who are more than 30 years old and have friends who are more than 30 years old.

# COMMAND ----------

g2 = g.filterEdges("relationship = 'friend'").filterVertices("age > 30").dropIsolatedVertices()

# COMMAND ----------

display(g2.vertices)

# COMMAND ----------

display(g2.edges)

# COMMAND ----------

# MAGIC %md ## Standard graph algorithms
# MAGIC
# MAGIC GraphFrames comes with a number of standard graph algorithms built in:
# MAGIC * Breadth-first search (BFS)
# MAGIC * Connected components
# MAGIC * Strongly connected components
# MAGIC * Label Propagation Algorithm (LPA)
# MAGIC * PageRank (regular and personalized)
# MAGIC * Shortest paths
# MAGIC * Triangle count

# COMMAND ----------

# MAGIC %md ###Breadth-first search (BFS)
# MAGIC
# MAGIC Search from "Esther" for users of age < 32.

# COMMAND ----------

paths = g.bfs("name = 'Esther'", "age < 32")
display(paths)

# COMMAND ----------

# MAGIC %md The search may also be limited by edge filters and maximum path lengths.

# COMMAND ----------

filteredPaths = g.bfs(
  fromExpr = "name = 'Esther'",
  toExpr = "age < 32",
  edgeFilter = "relationship != 'friend'",
  maxPathLength = 3)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md ## Connected components
# MAGIC
# MAGIC Compute the connected component membership of each vertex and return a DataFrame with each vertex assigned a component ID. The GraphFrames connected components implementation can take advantage of checkpointing to improve performance.

# COMMAND ----------

sc.setCheckpointDir("/tmp/graphframes-example-connected-components")
result = g.connectedComponents()
display(result)

# COMMAND ----------

# MAGIC %md ## Strongly connected components
# MAGIC
# MAGIC Compute the strongly connected component (SCC) of each vertex and return a DataFrame with each vertex assigned to the SCC containing that vertex.

# COMMAND ----------

result = g.stronglyConnectedComponents(maxIter=10)
display(result.select("id", "component"))

# COMMAND ----------

# MAGIC %md ## Label Propagation
# MAGIC
# MAGIC Run static Label Propagation Algorithm for detecting communities in networks.
# MAGIC
# MAGIC Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the most frequent community affiliation of incoming messages.
# MAGIC
# MAGIC LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

# COMMAND ----------

result = g.labelPropagation(maxIter=5)
display(result)

# COMMAND ----------

# MAGIC %md ## PageRank
# MAGIC
# MAGIC Identify important vertices in a graph based on connections.

# COMMAND ----------

results = g.pageRank(resetProbability=0.15, tol=0.01)
display(results.vertices)

# COMMAND ----------

display(results.edges)

# COMMAND ----------

# Run PageRank for a fixed number of iterations.
g.pageRank(resetProbability=0.15, maxIter=10)

# COMMAND ----------

# Run PageRank personalized for vertex "a"
g.pageRank(resetProbability=0.15, maxIter=10, sourceId="a")

# COMMAND ----------

# MAGIC %md ## Shortest paths
# MAGIC
# MAGIC Computes shortest paths to the given set of landmark vertices, where landmarks are specified by vertex ID.

# COMMAND ----------

results = g.shortestPaths(landmarks=["a", "d"])
display(results)

# COMMAND ----------

# MAGIC %md ###Triangle count
# MAGIC
# MAGIC Computes the number of triangles passing through each vertex.

# COMMAND ----------

results = g.triangleCount()
display(results)
