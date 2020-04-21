Elasticsearch DynaRank Plugin
=======================

## Overview

DynaRank Plugin provides a feature for Dynamic Ranking at a search time.
You can change top N documents in the search result with your re-ordering algorithm.
Elasticsearch has [rescoring](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-rescore.html "rescoring"), but DynaRank is different as below:

 * DynaRank's reranking is executed on requested node only, not on each shard. 
 * DynaRank uses a script language for reranking.


## Version

[Versions in Maven Repository](https://repo1.maven.org/maven2/org/codelibs/elasticsearch-dynarank/)

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-dynarank/issues "issue").

## Installation

    $ $ES_HOME/bin/elasticsearch-plugin install org.codelibs:elasticsearch-dynarank:7.6.0

## Getting Started

### Create Sample Data

Create "sample" index:

    $ COUNT=1;while [ $COUNT -le 100 ] ; do curl -XPOST 'localhost:9200/sample/_doc/' -d "{\"message\":\"Hello $COUNT\",\"counter\":$COUNT}";COUNT=`expr $COUNT + 1`; done

100 documents are inserted. You can see 10 documents by an ascending order of "counter" field:

    $ curl -XPOST "http://127.0.0.1:9200/sample/_search" -d'
    {
       "query": {
          "match_all": {}
       },
       "sort": [
          {
             "counter": {
                 "order": "asc"
             }
          }
       ]
    }'

### Enable Reranking

DynaRank plugin is enabled if your re-order script is set to the target index:

```
$ curl -s -XPUT -H 'Content-Type: application/json' "localhost:9200/sample/_settings" -d"
{
  \"index\" : {
    \"dynarank\":{
      \"script_sort\":{
        \"lang\": \"painless\",
        \"script\": \"def l=new ArrayList();for(def h:searchHits){l.add(h);}return l.stream().sorted((s1,s2)->s2.getSourceAsMap().get('counter')-s1.getSourceAsMap().get('counter')).toArray(n->new org.elasticsearch.search.SearchHit[n])\"
      },
      \"reorder_size\": 5
     }
  }
}"
```

This setting sorts top 5 documents (5 is given by reorder\_size) by a descending order of "counter" field, and others are by an ascending order.

### Disable Reranking

Set an empty value to index.dynarank.script\_sort.script:

```
$ curl -s -XPUT -H 'Content-Type: application/json' "localhost:9200/sample/_settings" -d"
{
  \"index\" : {
    \"dynarank\":{
      \"script_sort\":{
        \"script\": \"\"
      }
     }
  }
}"
```

## References

### dynarank\_diversity\_sort Script Sort

DynaRank plugin provides a sort feature for a diversity problem.
The sort script is dynarank\_diversity\_sort.
The configuration is below:

    curl -XPUT -H 'Content-Type: application/json' 'localhost:9200/sample/_settings' -d '
    {
      "index" : {
        "dynarank":{
          "script_sort":{
            "lang":"dynarank_diversity_sort",
            "params":{
              "diversity_fields":["filedname1", "filedname2"],
              "diversity_thresholds":[0.95, 1]
            }
          },
          "reorder_size":100
         }
      }
    }'

diversity\_fields is fields for a diversity.
diversity\_thresholds is a threshold for a similarity of each document.
