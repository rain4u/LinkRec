# LinkRec

Recommendation Engine + Ranking Engine

## Database - HBase

Every row is a line for link information

Two column families: 'link' and 'user'

1. 'link' family is for link properties like 'link:url', 'link:title', 'link:time'
2. 'user' family is for the users who shared this link. user_id is stored in the column 'user:user_id' ?

## Recommendation Engine

1. Load Training Data - From HBase
2. Load User Data
3. Find best model
4. Predict for user
5. Send recommendation result to Ranking Engine

## Ranking Engine

Ranking based on the following criteria:

1. Index in recommendation result
2. Popularity
3. Freshness
4. ...

## API

Send grabbed data to recommendation server
```
Request: POST /url with { user: id, links: [ { url: url, title: name, time: timestamp }, ... ] }
Response: -
```

Get recommendation result from server
```
Request: GET /url with { user: id }
Response: { reclinks: [ { url: url, title: name }, { url: url, title: name }, ... ] }
```

