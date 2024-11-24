# 1 Billion Row Challenge

## Introduction

This challenge revolves around the concept of processing a large dataset.
The dataset is a CSV file with 1 billion rows. 
The goal is to read the file and process it in a way that is both efficient and scalable.

Specified documentation, originally Java oriented: [1 Billion Row Challenge](https://github.com/gunnarmorling/1brc)

## Specific Improvements

- Chunk reading of file, instead of reading line by line
- Focus on byte processing, minimize conversions and implement custom float conversion


## Learnings 

I tried multiple parallelization approaches, but all of them actually slowed down the process.
Because the overhead of starting another thread for each chunk or each line was too high.