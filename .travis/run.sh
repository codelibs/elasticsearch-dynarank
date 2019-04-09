#!/bin/bash

cd `dirname $0`
cd ..

BASE_DIR=`pwd`
TEST_DIR=$BASE_DIR/target
ES_VERSION=`grep '<elasticsearch.version>' $BASE_DIR/pom.xml | sed -e "s/.*>\(.*\)<.*/\1/"`
ES_HOST=localhost
ES_PORT=9200
TMP_FILE=$TEST_DIR/tmp.$$

ZIP_FILE=$HOME/.m2/repository/elasticsearch-$ES_VERSION.zip
if [ ! -f $ZIP_FILE ] ; then
  curl -o $ZIP_FILE -L https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-$ES_VERSION.zip
fi

mkdir -p $TEST_DIR
cd $TEST_DIR

echo "Installing Elasticsearch..."
rm -rf elasticsearch-$ES_VERSION > /dev/null
unzip $ZIP_FILE
./elasticsearch-$ES_VERSION/bin/elasticsearch-plugin install org.codelibs:elasticsearch-minhash:6.7.0 -b
#./elasticsearch-$ES_VERSION/bin/elasticsearch-plugin install file:`ls $BASE_DIR/../elasticsearch-minhash/target/releases/elasticsearch-*.zip` -b
./elasticsearch-$ES_VERSION/bin/elasticsearch-plugin install file:`ls $BASE_DIR/target/releases/elasticsearch-*.zip` -b
echo "dynarank.cache.clean_interval: 1s" >> ./elasticsearch-$ES_VERSION/config/elasticsearch.yml

echo "Starting Elasticsearch..."
./elasticsearch-$ES_VERSION/bin/elasticsearch &
ES_PID=`echo $!`

RET=-1
COUNT=0
while [ $RET != 0 -a $COUNT -lt 60 ] ; do
  echo "Waiting for ${ES_HOST}..."
  curl --connect-timeout 60 --retry 10 -s "$ES_HOST:$ES_PORT/_cluster/health?wait_for_status=green&timeout=3m"
  RET=$?
  COUNT=`expr $COUNT + 1`
  sleep 1
done
curl "$ES_HOST:$ES_PORT"

echo "=== Start Testing ==="

curl -s -H "Content-Type: application/json" -XPUT "$ES_HOST:$ES_PORT/sample" -d '
{
  "mappings" : {
    "_doc" : {
      "properties" : {
        "counter" : {
          "type" : "long"
        },
        "id" : {
          "type" : "keyword"
        },
        "msg" : {
          "type" : "text"
        }
      }
    }
  },
  "settings" : {
    "index" : {
      "number_of_shards" : "5",
      "number_of_replicas" : "0",
      "dynarank" : {
        "reorder_size" : 100,
        "script_sort" : {
          "lang" : "painless",
          "script" : "java.util.Arrays.sort(params.searchHists, (s1,s2)-> s2.getSourceAsMap().get(\"counter\") - s1.getSourceAsMap().get(\"counter\"))",
          "params" : {
            "foo" : "bar"
          }
        }
      }
    }
  }
}
'


count=1
while [ $count -le 1000 ] ; do
  curl -s -H "Content-Type: application/json" -XPOST "$ES_HOST:$ES_PORT/sample/_doc/$count" -d "{\"id\":\"$count\",\"msg\":\"test $count\",\"counter\":$count}" > /dev/null
  count=`expr $count + 1`
done
curl -s -H "Content-Type: application/json" -XPOST "$ES_HOST:$ES_PORT/_refresh" > /dev/null
curl -s "$ES_HOST:$ES_PORT/_cat/indices?v"

echo "=== Finish Testing ==="

echo "Stopping Elasticsearch..."
kill $ES_PID
