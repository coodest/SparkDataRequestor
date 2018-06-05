# Introduction

This tool is used for data collection from various data source including web pages, online social networks and so on.

It is implemented under the concepts of paper "Yin Z., Yue K., Wu H., Su Y. Adaptive and Parallel Data Acquisition from Online Big Graphs//Proceedings of the International Conference on Database Systems for Advanced Applications. Gold Coast, Australia, Springer, LNCS 10827, 2018: 223-331."

# Prerequisite 
1. A running Spark cluster: this tool needs a Spark environment(1.6.1 or later) to collect the data on the Internet.
2. MySQL database.

# How to use
To use this tool:
1. Modify the the MySQL login info in util/DBOperator.scala
2. Custom the file in module/downloader/QMCSDownloader.scala to adapt the web page you want to crawl.
3. Submit the code to your Spark cluster.

# Contact

Email: 350526878@qq.com