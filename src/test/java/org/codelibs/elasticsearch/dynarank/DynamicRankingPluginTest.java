package org.codelibs.elasticsearch.dynarank;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker;
import org.codelibs.elasticsearch.dynarank.ranker.DynamicRanker.ScriptInfo;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class DynamicRankingPluginTest {
    ElasticsearchClusterRunner runner;

    private String clusterName;

    @Before
    public void setUp() throws Exception {
        clusterName = "es-dynarank-" + System.currentTimeMillis();
        runner = new ElasticsearchClusterRunner();
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("dynarank.cache.clean_interval", "1s");
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("discovery.type", "single-node");
                // settingsBuilder.putList("discovery.seed_hosts", "127.0.0.1:9301");
                // settingsBuilder.putList("cluster.initial_master_nodes", "127.0.0.1:9301");
            }
        }).build(newConfigs().numOfNode(1).clusterName(clusterName).pluginTypes(
                "org.codelibs.elasticsearch.dynarank.DynamicRankingPlugin" + ",org.codelibs.elasticsearch.minhash.MinHashPlugin"));
        runner.ensureGreen();
    }

    @After
    public void tearDown() throws Exception {
        runner.close();
        runner.clean();
    }

//    @Test
//    public void scriptInfoCache() throws Exception {
//
//        assertEquals(1, runner.getNodeSize());
//        final Client client = runner.client();
//
//        final String index = "sample";
//        final String alias = "test";
//        final String type = "data";
//        CreateIndexResponse createIndexResponse = runner.createIndex(index,
//                Settings.builder().put(DynamicRanker.SETTING_INDEX_DYNARANK_REORDER_SIZE.getKey(), 100)
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_LANG.getKey(), "painless")
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_SCRIPT.getKey(),
//                                "Arrays.sort(hits, (s1,s2)-> s2.getSourceAsMap().get(\"counter\") - s1.getSourceAsMap().get(\"counter\"))")
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_PARAMS.getKey() + "foo", "bar").build());
//        assertTrue(createIndexResponse.isAcknowledged());
//        AcknowledgedResponse aliasesResponse = runner.updateAlias(alias, new String[] { index }, null);
//        assertTrue(aliasesResponse.isAcknowledged());
//
//        for (int i = 1; i <= 1000; i++) {
//            final IndexResponse indexResponse1 = runner.insert(index, type, String.valueOf(i),
//                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"counter\":" + i + "}");
//            assertEquals(Result.CREATED, indexResponse1.getResult());
//        }
//
//        final DynamicRanker ranker = DynamicRanker.getInstance();
//        {
//            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
//                    .addSort("counter", SortOrder.ASC).execute().actionGet();
//            final SearchHits hits = searchResponse.getHits();
//            assertEquals(1000, hits.getTotalHits().value);
//            assertEquals(10, hits.getHits().length);
//            assertEquals("100", hits.getHits()[0].getId());
//            assertEquals("91", hits.getHits()[9].getId());
//        }
//
//        final ScriptInfo scriptInfo1 = ranker.getScriptInfo(index);
//        final ScriptInfo scriptInfo2 = ranker.getScriptInfo(index);
//        Thread.sleep(2000);
//        final ScriptInfo scriptInfo3 = ranker.getScriptInfo(index);
//        assertTrue(scriptInfo1 == scriptInfo2);
//        assertFalse(scriptInfo1 == scriptInfo3);
//
//        {
//            final SearchResponse searchResponse = client.prepareSearch(alias).setQuery(QueryBuilders.matchAllQuery())
//                    .addSort("counter", SortOrder.ASC).execute().actionGet();
//            final SearchHits hits = searchResponse.getHits();
//            assertEquals(1000, hits.getTotalHits().value);
//            assertEquals(10, hits.getHits().length);
//            assertEquals("100", hits.getHits()[0].getId());
//            assertEquals("91", hits.getHits()[9].getId());
//        }
//
//        final ScriptInfo scriptInfo4 = ranker.getScriptInfo(alias);
//        final ScriptInfo scriptInfo5 = ranker.getScriptInfo(alias);
//        Thread.sleep(2000);
//        final ScriptInfo scriptInfo6 = ranker.getScriptInfo(alias);
//        assertTrue(scriptInfo4 == scriptInfo5);
//        assertFalse(scriptInfo4 == scriptInfo6);
//    }

//    @Test
//    public void reorder() throws Exception {
//
//        assertEquals(1, runner.getNodeSize());
//        final Client client = runner.client();
//
//        final String index = "sample";
//        final String alias = "test";
//        final String type = "data";
//        CreateIndexResponse createIndexResponse = runner.createIndex(index,
//                Settings.builder().put(DynamicRanker.SETTING_INDEX_DYNARANK_REORDER_SIZE.getKey(), 100)
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_LANG.getKey(), "painless")
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_SCRIPT.getKey(),
//                                "Arrays.sort(hits, (s1,s2)-> s2.getSourceAsMap().get(\"counter\") - s1.getSourceAsMap().get(\"counter\"))")
//                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_PARAMS.getKey() + "foo", "bar").build());
//        assertTrue(createIndexResponse.isAcknowledged());
//        AcknowledgedResponse aliasesResponse = runner.updateAlias(alias, new String[] { index }, null);
//        assertTrue(aliasesResponse.isAcknowledged());
//
//        for (int i = 1; i <= 1000; i++) {
//            final IndexResponse indexResponse1 = runner.insert(index, type, String.valueOf(i),
//                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"counter\":" + i + "}");
//            assertEquals(Result.CREATED, indexResponse1.getResult());
//        }
//
//        assertResultOrder(client, index);
//        assertResultOrder(client, alias);
//
//        String index2 = index + "2";
//        runner.createIndex(index2, (Settings) null);
//        runner.updateAlias(alias, new String[] { index2 }, null);
//        int tempId = 99999;
//        runner.insert(index2, type, String.valueOf(tempId),
//                "{\"id\":\"" + tempId + "\",\"msg\":\"test " + tempId + "\",\"counter\":" + tempId + "}");
//        runner.delete(index2, type, String.valueOf(tempId));
//        runner.refresh();
//        assertResultOrder(client, alias);
//    }

    private void assertResultOrder(Client client, String index) {
        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("100", hits.getHits()[0].getId());
            assertEquals("91", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(50).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("50", hits.getHits()[0].getId());
            assertEquals("41", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(90).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("10", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(91).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("9", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[8].getId());
            assertEquals("101", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(95).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("5", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[4].getId());
            assertEquals("101", hits.getHits()[5].getId());
            assertEquals("105", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(99).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("1", hits.getHits()[0].getId());
            assertEquals("101", hits.getHits()[1].getId());
            assertEquals("109", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setFrom(100).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("101", hits.getHits()[0].getId());
            assertEquals("110", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(0).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("20", hits.getHits()[0].getId());
            assertEquals("11", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("10", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(11).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(9, hits.getHits().length);
            assertEquals("9", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[8].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter"))
                    .addSort("counter", SortOrder.ASC).setFrom(0).setSize(101).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(101, hits.getHits().length);
            assertEquals("100", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[99].getId());
            assertEquals("101", hits.getHits()[100].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(0).setSize(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("20", hits.getHits()[0].getId());
            assertEquals("11", hits.getHits()[9].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(15).setSize(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(5, hits.getHits().length);
            assertEquals("5", hits.getHits()[0].getId());
            assertEquals("1", hits.getHits()[4].getId());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(20).setSize(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(0, hits.getHits().length);
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addSort("counter", SortOrder.ASC).setFrom(21).setSize(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits().value);
            assertEquals(0, hits.getHits().length);
        }

        {
            final SearchResponse searchResponse = runner.search(index, QueryBuilders.queryStringQuery("msg:foo"), null, 0, 10);
            final SearchHits hits = searchResponse.getHits();
            assertEquals(0, hits.getTotalHits().value);
        }

        for (int i = 0; i < 1000; i++) {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("100", hits.getHits()[0].getId());
            assertEquals("91", hits.getHits()[9].getId());
        }
    }

    @Test
    public void standardBucketFactory() throws Exception {

        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"diversity_fields\":[\"category\"],\"diversity_thresholds\":[0.95,1],\"reorder_size\":20}}}}"
                    + "}";
            runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .endObject()//

                    // category
                    .startObject("category")//
                    .field("type", "keyword")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        insertTestData(index, 1, "aaa bbb ccc", "cat1");
        insertTestData(index, 2, "aaa bbb ccc", "cat1");
        insertTestData(index, 3, "aaa bbb ccc", "cat2");
        insertTestData(index, 4, "aaa bbb ddd", "cat1");
        insertTestData(index, 5, "aaa bbb ddd", "cat2");
        insertTestData(index, 6, "aaa bbb ddd", "cat2");
        insertTestData(index, 7, "aaa bbb eee", "cat1");
        insertTestData(index, 8, "aaa bbb eee", "cat1");
        insertTestData(index, 9, "aaa bbb eee", "cat2");
        insertTestData(index, 10, "aaa bbb fff", "cat1");

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "category")
                    .setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(10, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("3", hits[1].getSourceAsMap().get("id"));
            assertEquals("2", hits[2].getSourceAsMap().get("id"));
            assertEquals("5", hits[3].getSourceAsMap().get("id"));
            assertEquals("4", hits[4].getSourceAsMap().get("id"));
            assertEquals("6", hits[5].getSourceAsMap().get("id"));
            assertEquals("7", hits[6].getSourceAsMap().get("id"));
            assertEquals("9", hits[7].getSourceAsMap().get("id"));
            assertEquals("8", hits[8].getSourceAsMap().get("id"));
            assertEquals("10", hits[9].getSourceAsMap().get("id"));
        }

        // disable rerank
        {
            final SearchResponse response = runner.client().prepareSearch("_all").setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(10, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("2", hits[1].getSourceAsMap().get("id"));
            assertEquals("3", hits[2].getSourceAsMap().get("id"));
            assertEquals("4", hits[3].getSourceAsMap().get("id"));
            assertEquals("5", hits[4].getSourceAsMap().get("id"));
            assertEquals("6", hits[5].getSourceAsMap().get("id"));
            assertEquals("7", hits[6].getSourceAsMap().get("id"));
            assertEquals("8", hits[7].getSourceAsMap().get("id"));
            assertEquals("9", hits[8].getSourceAsMap().get("id"));
            assertEquals("10", hits[9].getSourceAsMap().get("id"));
        }
    }

    @Test
    public void diversityMultiSort() throws Exception {

        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhash\"]}" + "},\"filter\":{"
                    + "\"my_minhash\":{\"type\":\"minhash\",\"seed\":1000}" + "}}},"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"bucket_factory\":\"minhash\",\"diversity_fields\":[\"minhash_value\",\"category\"],\"diversity_thresholds\":[0.95,1]}},\"reorder_size\":20}"
                    + "}";
            runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", "minhash_value")//
                    .endObject()//

                    // category
                    .startObject("category")//
                    .field("type", "keyword")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        insertTestData(index, 1, "aaa bbb ccc", "cat1");
        insertTestData(index, 2, "aaa bbb ccc", "cat1");
        insertTestData(index, 3, "aaa bbb ccc", "cat2");
        insertTestData(index, 4, "aaa bbb ddd", "cat1");
        insertTestData(index, 5, "aaa bbb ddd", "cat2");
        insertTestData(index, 6, "aaa bbb ddd", "cat2");
        insertTestData(index, 7, "aaa bbb eee", "cat1");
        insertTestData(index, 8, "aaa bbb eee", "cat1");
        insertTestData(index, 9, "aaa bbb eee", "cat2");
        insertTestData(index, 10, "aaa bbb fff", "cat1");

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value", "category")
                    .setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(10, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("5", hits[1].getSourceAsMap().get("id"));
            assertEquals("7", hits[2].getSourceAsMap().get("id"));
            assertEquals("10", hits[3].getSourceAsMap().get("id"));
            assertEquals("3", hits[4].getSourceAsMap().get("id"));
            assertEquals("4", hits[5].getSourceAsMap().get("id"));
            assertEquals("9", hits[6].getSourceAsMap().get("id"));
            assertEquals("2", hits[7].getSourceAsMap().get("id"));
            assertEquals("6", hits[8].getSourceAsMap().get("id"));
            assertEquals("8", hits[9].getSourceAsMap().get("id"));
        }

        // disable rerank
        {
            final SearchResponse response = runner.client().prepareSearch("_all").setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(10, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("2", hits[1].getSourceAsMap().get("id"));
            assertEquals("3", hits[2].getSourceAsMap().get("id"));
            assertEquals("4", hits[3].getSourceAsMap().get("id"));
            assertEquals("5", hits[4].getSourceAsMap().get("id"));
            assertEquals("6", hits[5].getSourceAsMap().get("id"));
            assertEquals("7", hits[6].getSourceAsMap().get("id"));
            assertEquals("8", hits[7].getSourceAsMap().get("id"));
            assertEquals("9", hits[8].getSourceAsMap().get("id"));
            assertEquals("10", hits[9].getSourceAsMap().get("id"));
        }
    }

    private void insertTestData(final String index, final int id, final String msg, final String category) {
        assertEquals(Result.CREATED,
                runner.insert(index, String.valueOf(id),
                        "{\"id\":\"" + id + "\",\"msg\":\"" + msg + "\",\"category\":\"" + category + "\",\"order\":" + id + "}")
                        .getResult());

    }

    @Test
    public void diversitySort() throws Exception {

        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhash\"]}" + "},\"filter\":{"
                    + "\"my_minhash\":{\"type\":\"minhash\",\"seed\":1000}" + "}}},"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"bucket_factory\":\"minhash\",\"diversity_fields\":[\"minhash_value\"],\"diversity_thresholds\":[0.95]}},\"reorder_size\":20}"
                    + "}";
            runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", "minhash_value")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        final StringBuilder[] texts = createTexts();
        for (int i = 1; i <= 100; i++) {
            // System.out.println(texts[i - 1]);
            final IndexResponse indexResponse1 = runner.insert(index, String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"" + texts[i - 1].toString() + "\",\"order\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.idsQuery().addIds("0"))
                    .storedFields("_source", "minhash_value").setFrom(20).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(0, searchHits.getTotalHits().value);
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(0)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("7", hits[1].getSourceAsMap().get("id"));
            assertEquals("10", hits[2].getSourceAsMap().get("id"));
            assertEquals("13", hits[3].getSourceAsMap().get("id"));
            assertEquals("18", hits[4].getSourceAsMap().get("id"));
            assertEquals("2", hits[5].getSourceAsMap().get("id"));
            assertEquals("8", hits[6].getSourceAsMap().get("id"));
            assertEquals("11", hits[7].getSourceAsMap().get("id"));
            assertEquals("14", hits[8].getSourceAsMap().get("id"));
            assertEquals("19", hits[9].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(10)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("3", hits[0].getSourceAsMap().get("id"));
            assertEquals("9", hits[1].getSourceAsMap().get("id"));
            assertEquals("12", hits[2].getSourceAsMap().get("id"));
            assertEquals("15", hits[3].getSourceAsMap().get("id"));
            assertEquals("20", hits[4].getSourceAsMap().get("id"));
            assertEquals("4", hits[5].getSourceAsMap().get("id"));
            assertEquals("16", hits[6].getSourceAsMap().get("id"));
            assertEquals("5", hits[7].getSourceAsMap().get("id"));
            assertEquals("17", hits[8].getSourceAsMap().get("id"));
            assertEquals("6", hits[9].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(20)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("21", hits[0].getSourceAsMap().get("id"));
            assertEquals("22", hits[1].getSourceAsMap().get("id"));
            assertEquals("23", hits[2].getSourceAsMap().get("id"));
            assertEquals("24", hits[3].getSourceAsMap().get("id"));
            assertEquals("25", hits[4].getSourceAsMap().get("id"));
            assertEquals("26", hits[5].getSourceAsMap().get("id"));
            assertEquals("27", hits[6].getSourceAsMap().get("id"));
            assertEquals("28", hits[7].getSourceAsMap().get("id"));
            assertEquals("29", hits[8].getSourceAsMap().get("id"));
            assertEquals("30", hits[9].getSourceAsMap().get("id"));
        }

        final BoolQueryBuilder testQuery = QueryBuilders.boolQuery().should(QueryBuilders.rangeQuery("order").from(1).to(5))
                .should(QueryBuilders.termQuery("order", 20)).should(QueryBuilders.termQuery("order", 30))
                .should(QueryBuilders.termQuery("order", 40)).should(QueryBuilders.termQuery("order", 50))
                .should(QueryBuilders.termQuery("order", 60)).should(QueryBuilders.termQuery("order", 70))
                .should(QueryBuilders.rangeQuery("order").from(80).to(90));
        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(testQuery)
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(0)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(22, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("20", hits[1].getSourceAsMap().get("id"));
            assertEquals("30", hits[2].getSourceAsMap().get("id"));
            assertEquals("40", hits[3].getSourceAsMap().get("id"));
            assertEquals("50", hits[4].getSourceAsMap().get("id"));
            assertEquals("60", hits[5].getSourceAsMap().get("id"));
            assertEquals("70", hits[6].getSourceAsMap().get("id"));
            assertEquals("82", hits[7].getSourceAsMap().get("id"));
            assertEquals("87", hits[8].getSourceAsMap().get("id"));
            assertEquals("2", hits[9].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(testQuery)
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(10)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(22, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("80", hits[0].getSourceAsMap().get("id"));
            assertEquals("83", hits[1].getSourceAsMap().get("id"));
            assertEquals("88", hits[2].getSourceAsMap().get("id"));
            assertEquals("3", hits[3].getSourceAsMap().get("id"));
            assertEquals("81", hits[4].getSourceAsMap().get("id"));
            assertEquals("84", hits[5].getSourceAsMap().get("id"));
            assertEquals("4", hits[6].getSourceAsMap().get("id"));
            assertEquals("85", hits[7].getSourceAsMap().get("id"));
            assertEquals("5", hits[8].getSourceAsMap().get("id"));
            assertEquals("86", hits[9].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(testQuery)
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(20)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(22, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("89", hits[0].getSourceAsMap().get("id"));
            assertEquals("90", hits[1].getSourceAsMap().get("id"));
        }

        for (int i = 0; i < 1000; i++) {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value").setFrom(0)
                    .setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("7", hits[1].getSourceAsMap().get("id"));
            assertEquals("10", hits[2].getSourceAsMap().get("id"));
            assertEquals("13", hits[3].getSourceAsMap().get("id"));
            assertEquals("18", hits[4].getSourceAsMap().get("id"));
            assertEquals("2", hits[5].getSourceAsMap().get("id"));
            assertEquals("8", hits[6].getSourceAsMap().get("id"));
            assertEquals("11", hits[7].getSourceAsMap().get("id"));
            assertEquals("14", hits[8].getSourceAsMap().get("id"));
            assertEquals("19", hits[9].getSourceAsMap().get("id"));
        }

    }

//    @Test
//    public void diversitySortWithShuffleMin() throws Exception {
//        diversitySortWithShuffle("min_bucket_threshold");
//    }
//
//    @Test
//    public void diversitySortWithShuffleMax() throws Exception {
//        diversitySortWithShuffle("max_bucket_threshold");
//    }

    private void diversitySortWithShuffle(String name) throws Exception {
        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhash\"]}" + "},\"filter\":{"
                    + "\"my_minhash\":{\"type\":\"minhash\",\"seed\":1000}" + "}}},"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"bucket_factory\":\"minhash\",\"diversity_fields\":[\"minhash_value\"],\"diversity_thresholds\":[0.95],\""
                    + name + "\":\"1\",\"shuffle_seed\":\"1\"}},\"reorder_size\":10}" + "}";
            runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", "minhash_value")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 200 documents
        final StringBuilder[] texts = createTexts();
        for (int i = 1; i <= 200; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"" + texts[(i - 1) % 10].toString() + "\",\"order\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        {
            final SearchResponse response = runner.client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("msg", "aaa0")).storedFields("_source", "minhash_value")
                    .addSort("_score", SortOrder.DESC).addSort("order", SortOrder.ASC).setFrom(0).setSize(5).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(20, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("21", hits[0].getSourceAsMap().get("id"));
            assertEquals("161", hits[1].getSourceAsMap().get("id"));
            assertEquals("81", hits[2].getSourceAsMap().get("id"));
            assertEquals("51", hits[3].getSourceAsMap().get("id"));
            assertEquals("191", hits[4].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("msg", "aaa0")).storedFields("_source", "minhash_value")
                    .addSort("_score", SortOrder.DESC).addSort("order", SortOrder.ASC).setFrom(5).setSize(5).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(20, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("111", hits[0].getSourceAsMap().get("id"));
            assertEquals("61", hits[1].getSourceAsMap().get("id"));
            assertEquals("151", hits[2].getSourceAsMap().get("id"));
            assertEquals("131", hits[3].getSourceAsMap().get("id"));
            assertEquals("71", hits[4].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("msg", "aaa0")).storedFields("_source", "minhash_value")
                    .addSort("_score", SortOrder.DESC).addSort("order", SortOrder.ASC).setFrom(10).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(20, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("91", hits[0].getSourceAsMap().get("id"));
            assertEquals("141", hits[1].getSourceAsMap().get("id"));
            assertEquals("151", hits[2].getSourceAsMap().get("id"));
            assertEquals("171", hits[3].getSourceAsMap().get("id"));
            assertEquals("1", hits[4].getSourceAsMap().get("id"));
            assertEquals("71", hits[5].getSourceAsMap().get("id"));
            assertEquals("81", hits[6].getSourceAsMap().get("id"));
            assertEquals("111", hits[7].getSourceAsMap().get("id"));
            assertEquals("121", hits[8].getSourceAsMap().get("id"));
            assertEquals("181", hits[9].getSourceAsMap().get("id"));
        }
    }

    private StringBuilder[] createTexts() {
        final StringBuilder[] texts = new StringBuilder[100];
        for (int i = 0; i < 100; i++) {
            texts[i] = new StringBuilder();
        }
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                if (i - j >= 0) {
                    texts[j].append(" aaa" + i);
                } else {
                    texts[j].append(" bbb" + i);
                }
            }
        }
        return texts;
    }

    @Test
    public void skipReorder() throws Exception {

        assertEquals(1, runner.getNodeSize());
        final Client client = runner.client();

        final String index = "sample";
        runner.createIndex(index,
                Settings.builder().put(DynamicRanker.SETTING_INDEX_DYNARANK_REORDER_SIZE.getKey(), 100)
                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_SCRIPT.getKey(),
                                "searchHits.sort {s1, s2 -> s2.getSourceAsMap().get('counter') - s1.getSourceAsMap().get('counter')} as org.elasticsearch.search.SearchHit[]")
                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_PARAMS.getKey() + "foo", "bar").build());

        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"counter\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        {
            final SearchResponse searchResponse = client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort("counter", SortOrder.ASC).setScroll("1m").execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("1", hits.getHits()[0].getId());
            assertEquals("10", hits.getHits()[9].getId());
        }

    }

    @Test
    public void skipReorder_scrollSearch() throws Exception {

        assertEquals(1, runner.getNodeSize());
        final Client client = runner.client();

        final String index = "sample";
        runner.createIndex(index,
                Settings.builder().put(DynamicRanker.SETTING_INDEX_DYNARANK_REORDER_SIZE.getKey(), 100)
                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_SCRIPT.getKey(),
                                "Arrays.sort(hits, (s1,s2)-> s2.getSourceAsMap().get(\"counter\") - s1.getSourceAsMap().get(\"counter\"))")
                        .put(DynamicRanker.SETTING_INDEX_DYNARANK_PARAMS.getKey() + "foo", "bar").build());

        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"test " + i + "\",\"counter\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        {
            final SearchResponse searchResponse =
                    client.prepareSearch(index).setQuery(QueryBuilders.matchAllQuery()).addSort("counter", SortOrder.ASC)
                            //.putHeader("_rerank", false)
                            .execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits().value);
            assertEquals(10, hits.getHits().length);
            assertEquals("1", hits.getHits()[0].getId());
            assertEquals("10", hits.getHits()[9].getId());
        }

    }

    @Test
    public void reorder_with_ignored() throws Exception {
        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhash\"]}" + "},\"filter\":{"
                    + "\"my_minhash\":{\"type\":\"minhash\",\"seed\":1000}" + "}}},"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"bucket_factory\":\"minhash\",\"diversity_fields\":[\"minhash_value\",\"category\"],\"diversity_thresholds\":[0.95,1],\"category_ignored_objects\":[\"category1\"]}},\"reorder_size\":20}"
                    + "}";
            runner.createIndex(index, Settings.builder().loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", "minhash_value")//
                    .endObject()//

                    // category
                    .startObject("category")//
                    .field("type", "keyword")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        final StringBuilder[] texts = createTexts();
        for (int i = 1; i <= 100; i++) {
            // System.out.println(texts[i - 1]);
            final IndexResponse indexResponse1 = runner.insert(index, String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\""
                    + texts[i - 1].toString() + "\",\"category\":\"category" + (i % 2) + "\",\"order\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        {
            final SearchResponse response = runner.client().prepareSearch(index).setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order").order(SortOrder.ASC)).storedFields("_source", "minhash_value", "category")
                    .setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("7", hits[1].getSourceAsMap().get("id"));
            assertEquals("11", hits[2].getSourceAsMap().get("id"));
            assertEquals("17", hits[3].getSourceAsMap().get("id"));
            assertEquals("2", hits[4].getSourceAsMap().get("id"));
            assertEquals("9", hits[5].getSourceAsMap().get("id"));
            assertEquals("13", hits[6].getSourceAsMap().get("id"));
            assertEquals("19", hits[7].getSourceAsMap().get("id"));
            assertEquals("3", hits[8].getSourceAsMap().get("id"));
            assertEquals("8", hits[9].getSourceAsMap().get("id"));
        }

    }

    @Test
    public void keepTop5() throws Exception {
        final String index = "test_index";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhash\"]}"
                    + "},\"filter\":{"
                    + "\"my_minhash\":{\"type\":\"minhash\",\"seed\":1000}"
                    + "}}},"
                    + "\"dynarank\":{\"script_sort\":{\"lang\":\"dynarank_diversity_sort\",\"params\":{\"bucket_factory\":\"minhash\",\"diversity_fields\":[\"minhash_value\",\"category\"],\"diversity_thresholds\":[0.95,1],\"category_ignored_objects\":[\"category1\"]}},\"reorder_size\":20,\"keep_topn\":5}"
                    + "}";
            runner.createIndex(index, Settings.builder()
                    .loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", "minhash_value")//
                    .endObject()//

                    // category
                    .startObject("category")//
                    .field("type", "keyword")//
                    .endObject()//

                    // order
                    .startObject("order")//
                    .field("type", "long")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        final StringBuilder[] texts = createTexts();
        for (int i = 1; i <= 100; i++) {
            // System.out.println(texts[i - 1]);
            final IndexResponse indexResponse1 = runner.insert(index,
                    String.valueOf(i),
                    "{\"id\":\"" + i + "\",\"msg\":\"" + texts[i - 1].toString()
                            + "\",\"category\":\"category" + (i % 2)
                            + "\",\"order\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        {
            final SearchResponse response = runner.client().prepareSearch(index)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addSort(SortBuilders.fieldSort("order")
                            .order(SortOrder.ASC))
                    .storedFields("_source", "minhash_value", "category")
                    .setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(100, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
            assertEquals("2", hits[1].getSourceAsMap().get("id"));
            assertEquals("3", hits[2].getSourceAsMap().get("id"));
            assertEquals("4", hits[3].getSourceAsMap().get("id"));
            assertEquals("5", hits[4].getSourceAsMap().get("id"));
            assertEquals("6", hits[5].getSourceAsMap().get("id"));
            assertEquals("9", hits[6].getSourceAsMap().get("id"));
            assertEquals("13", hits[7].getSourceAsMap().get("id"));
            assertEquals("19", hits[8].getSourceAsMap().get("id"));
            assertEquals("7", hits[9].getSourceAsMap().get("id"));
        }

        {
            final SearchResponse response = runner.client().prepareSearch(index)
                    .setQuery(QueryBuilders.termQuery("id", "1"))
                    .addSort(SortBuilders.fieldSort("order")
                            .order(SortOrder.ASC))
                    .storedFields("_source", "minhash_value", "category")
                    .setFrom(0).setSize(10).execute().actionGet();
            final SearchHits searchHits = response.getHits();
            assertEquals(1, searchHits.getTotalHits().value);
            final SearchHit[] hits = searchHits.getHits();
            assertEquals("1", hits[0].getSourceAsMap().get("id"));
        }
    }
}
