# Versatile Percentiles
## Fast, Efficient Latency Monitoring

### Motivation 

Latency monitoring is of critical importance for any website or application - an annoying user experience can be a death sentence for an otherwise enjoyable app. 

This project explores an application of the t-digest data structure, introduced by Dunning and Ertl (repo here: https://github.com/tdunning/t-digest). 

The t-digest data structure stores a compressed version of a collection of 1-D data. The structure allows for fast computation of percentiles and is mergeable, allowing for natural parallel computations on a platform like Spark. 

Presentation slides can be found here: bit.ly/2sZoe3I

### 
