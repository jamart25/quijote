# -*- coding: utf-8 -*-
"""
Created on Thu May  4 12:47:16 2023

@author: Nanoi O-Yama
"""

from pyspark import SparkContext
import re
import sys



def counting_words(file_name):
    sc = SparkContext.getOrCreate()
    text_file = sc.textFile(file_name)
    counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)
                           
    output = counts.collect()
    out = ""
    
    for (word, count) in output:
        out = word + ": " + str(count) + "\n"
    
    out_file = "out_" + file_name
    with open(out_file, "w") as f:
    	f.write.content(out)
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        counting_words(sys.argv[1])

"""
if __name__ == "__main__":
    
    num_words = counting_words("quijote_s05.txt")
    with open("out_quijote_s05.txt", "w", encoding = "UTF-8") as f:
        f.write(str(num_words))
    num_words = counting_words("quijote.txt")
    with open("out_quijote.txt", "w", encoding = "UTF-8") as f:
        f.write(str(num_words))
"""
