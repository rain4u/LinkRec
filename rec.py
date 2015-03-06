import pandas as pd
import numpy as np
import pylab as pl

rnames = ['user_id', 'movie_id', 'rating', 'timestamp']
ratings = pd.read_table("ml-100k/u.data", header=None, names=rnames)

def rec_for_user(uid):
	rated_movies = ratings[ratings.user_id==uid].movie_id.unique()
	mask = (ratings.movie_id.isin(rated_movies)) & (ratings.user_id!=uid)
	related_users = ratings[mask].sort('user_id')
	related_users['user_similarity'] = related_users.groupby('user_id')['movie_id'].transform('count')
	# user_similarity.sort('count')
	user_similarity = related_users[['user_id', 'user_similarity']][related_users.user_id.duplicated()==False].sort('user_similarity', ascending=False)

	most_similar_user = user_similarity['user_id'].iloc[0]
	mask = (~ratings.movie_id.isin(rated_movies)) & (ratings.user_id==most_similar_user)

	return ratings[mask].movie_id.unique()


print rec_for_user(196)

