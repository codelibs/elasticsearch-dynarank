Elasticsearch DynaRank Pugin
=======================

## Overview

DynaRank Plugin provides a feature for Dynamic Ranking at a search time.
You can change top N documents in the search result with your re-ordering algorism.

## Version

| Taste     | Elasticsearch |
|:---------:|:-------------:|
| master    | 1.3.X         |
| 1.3.0     | 1.3.2         |

### Issues/Questions

Please file an [issue](https://github.com/codelibs/elasticsearch-dynarank/issues "issue").
(Japanese forum is [here](https://github.com/codelibs/codelibs-ja-forum "here").)

## Installation

### Install DynaRank Plugin

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-dynarank/1.3.0

## Getting Started

### Create Sample Data

Create "sample" index:

    $ COUNT=1;while [ $COUNT -le 100 ] ; do curl -XPOST 'localhost:9200/sample/data' -d "{\"message\":\"Hello $COUNT\",\"counter\":$COUNT}";COUNT=`expr $COUNT + 1`; done

100 documents are inserted. You can see 10 documents by an ascending order of "counter" field:

    $ curl -XPOST "http://127.0.0.1:9200/sample/data/_search" -d'
    {
       "query": {
          "match_all": {}
       },
       "fields": [
          "counter",
          "_source"
       ],
       "sort": [
          {
             "counter": {
                 "order": "asc"
             }
          }
       ]
    }'

### Enable DynaRank Plugin

DynaRank plugin is enabled if your re-order script is set to the target index:

    $ curl -XPOST 'localhost:9200/sample/_close'
    $ curl -XPUT 'localhost:9200/sample/_settings?index.dynarank.script_sort.script=searchHits.sort%20%7Bs1%2C%20s2%20-%3E%20s2.field%28%27counter%27%29.value%28%29%20-%20s1.field%28%27counter%27%29.value%28%29%7D%20as%20org.elasticsearch.search.internal.InternalSearchHit%5B%5D'
    $ curl -XPUT 'localhost:9200/sample/_settings?index.dynarank.reorder_size=5'
    $ curl -XPOST 'localhost:9200/sample/_open'

The above script is:

    searchHits.sort {s1, s2 -> s2.field('counter').value() - s1.field('counter').value()} as org.elasticsearch.search.internal.InternalSearchHit[]

This setting sorts top 5 documents (5 is given by reorder\_size) by a descending order of "counter" field, and others are by an ascending order.


