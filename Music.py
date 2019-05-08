from pyspark import SparkContext
from ml_utils import *
import argparse
import os
import nltk

nltk.download('punkt')
os.environ['JAVA_HOME'] = "/Library/Java/JavaVirtualMachines/jdk1.8.0_211.jdk/Contents/Home"
from operator import add
class Music(object):
    def __init__(self):
        sc = SparkContext("local[4]", "simple")
        original = sc.textFile("file:///home/hadoop/COMP5349-Assignment2/amazon_reviews_us_Music_v1_00.tsv")
        header = original.first()
        original = original.filter(lambda x: x != header)
        # eliminate the first row
        self.og = original
        self.Reviews = original.map(countReview)
        self.Customers = original.map(countCustomer).reduceByKey(add)
        self.Products = original.map(countProduct).reduceByKey(add)
