Item-based-Collaborative-Filtering-for-Recommendation
=====================================================

 
Goal:

 It is to understand item-based collaborative ﬁltering for building an efﬁcient recommendation system under a large amount of movie rating data: Netﬂix prize problem
 
 Steps:
 Generated my own data with the following format. 
 
 users, movies, rating 
 
 Mapper and Reducer will compute similarity scores for all pairs of movies. The input is the formatted training dataset
after 2nd step and the output are ﬁles in which each line would be like:
movieID_1, movieID_2, similarity_score. (Excluded movieID_2, movieID_1,
similarity_score, because they are the same. The item similarity matrix is symmetric.)


The key-value pairs of two mappers and reducers are described below:

First mapper:
-------------
customerID, movieID, rating ) key: customerID, value: (movieID, rating)
Your ﬁrst reducer:
customerID, (movieID, rating) ) key: customerID, value would be a list of (movieID,
rating): (movieID_1, rating_1) (movieID_2, rating_2) ... (movieID_n, rating_n)
Second mapper:
--------------
customerID, (movieID_1, rating_1) (movieID_2, rating_2)...(movieID_n, rating_n) )
key: a pair of movies: (movieID_i, movie_j), value: a pair of corresponding ratings: 
rating_i, rating_j)
Second reducer:
---------------
(movieID_i, movie_j), (rating_i, rating_j) ) key: a pair of movies: (movieID_i, movieID_j),
value would be similarity score: S[ij].

The  job (only have Mapper, no Reducer): read the training data to output all
records related to a given customer ID (e.g. CID). And the third job (only have Mapper,
no Reducer): read the item similarity score ﬁle generated from step 3 and output all
records related to a given movie ID (e.g. MID)
