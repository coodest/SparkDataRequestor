# Introduction

This tool is used for adaptive and parallel data acquisition from various online data sources including web pages, social networks and so on.

It is implemented following the concepts and algorithms in the paper "Yin Z., Yue K., Wu H., Su Y. Adaptive and Parallel Data Acquisition from Online Big Graphs. Proceedings of the International Conference on Database Systems for Advanced Applications, Gold Coast, Australia, Springer, LNCS 10827, 2018, pp.223-331."

# Prerequisite 
1. A running Spark cluster: this tool needs a Spark environment (1.6.1 or later) to collect the online data.
2. MySQL database.

# How to use
To use this tool:
1. Modify the MySQL login information in util/DBOperator.scala
2. Custom the file in module/downloader/QMCSDownloader.scala to adapt the web pages that you want to crawl
3. Submit the code to your Spark cluster

# Contact

Email: ziduyin@ynu.edu.cn
