# LinkRec

Recommendation Engine + Ranking Engine

# Recommendation Engine

1. Load Training Data - From HBase
2. Load User Data
3. Find best model
4. Predict for user
5. Send recommendation result to Ranking Engine

# Ranking Engine

Ranking based on the following criteria:

1. Index in recommendation result
2. Popularity
3. Freshness
4. ...

# API

Send grabbed data to recommendation server
```
Request: POST /url with { user: id, link: url, title: name, date: timestamp }
Response: -
```

Get recommendation result from server
```
Request: GET /url with { user: id }
Response: { reclinks: [ { link: url, title: name }, { link: url, title: name }, ... ] }
```

