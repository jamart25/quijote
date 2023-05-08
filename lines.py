# -*- coding: utf-8 -*-
"""
Created on Thu May  4 12:20:36 2023

@author: Nanoi O-Yama
"""
from pyspark import SparkContext
import sys
import random

def extract_lines(file_name, percentage):
    sc = SparkContext.getOrCreate()
    text_file = sc.textFile(file_name)
    lines = text_file.collect()
    out = ""
    for line in lines:
        dice = random.uniform(0, 1)
        if dice >= percentage:
            out = out + line + "\n"
    return out

def create_file(content, name):
    with open(name, "w") as f:
        f.write(content)
        
def main(file_name, out_file_name):
    text = extract_lines(file_name, 0.6)
    create_file(text, out_file_name)

    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1], sys.argv[2])
    
