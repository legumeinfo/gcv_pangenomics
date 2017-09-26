#! /usr/bin/env python

from afs     import approximateFrequentSubpaths
from db      import Neo4j
from pyspark import SparkConf, SparkContext


def getSparkContext(appName="Genome Context Viewer", master="local[2]"):
  conf = SparkConf().setAppName(appName).setMaster(master)
  sc   = SparkContext.getOrCreate(conf)
  sc.setLogLevel("ERROR")
  sc.setCheckpointDir("./checkpoints")
  return sc


if __name__ == '__main__':
  sc       = getSparkContext()
  db       = Neo4j(sc)
  paths, g = db.loadGeneGraph()
  paths.cache()
  g.cache()
  #q = 1028  # medtr.chr6
  q       = 2
  maxgap  = 10
  minsize = 2
  afs    = approximateFrequentSubpaths(sc, paths, q, maxgap, minsize)
