# LinkRec

Recommendation Engine + Ranking Engine

## Database - HBase

Every row is a line for user link sharing information

1. Row key is the user id
2. Column is the 'link' family followed by link url
3. Cell timestamp is the time provided in input which can be the share time of certain link by certain user
4. Cell value is the title of the link

|       | link:url1 | link:url2 | ... |
|-------|:-------------:|------:|------:|
| user1 | timestamp:time11, value:title1 | timestamp:time12, value:title2 | ... |
| user2 |  | timestamp:time22, value:title2 | ... |
| user3 | timestamp:time31, value:title1 |  | ... |

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
Request: POST /sendLink with { user: id, links: [ { url: url, title: name, time: timestamp }, ... ] }
Response: 200 if success, otherwise failure; body contains error message
```

Get recommendation result from server
```
Request: GET /getRec with { user: id }
Response: { reclinks: [ { url: url, title: name }, { url: url, title: name }, ... ] }
```

Reset data in hbase (test only)
```
Request: DELETE /reset
Response: 200 if success, otherwise failure; body contains error message
```


