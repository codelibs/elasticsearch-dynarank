package org.codelibs.dynarank;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.codelibs.dynarank.ranker.DynamicRanker;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DynamicRankingPluginTest {
    ElasticsearchClusterRunner runner;

    @Before
    public void setUp() throws Exception {
        runner = new ElasticsearchClusterRunner();
        runner.build(new String[] { "-numOfNode", "1", "-indexStoreType", "ram" });
        runner.ensureGreen();
    }

    @After
    public void tearDown() throws Exception {
        runner.close();
        runner.clean();
    }

    @Test
    public void reorder() throws Exception {

        assertThat(1, is(runner.getNodeSize()));
        final Client client = runner.client();

        final String index = "sample";
        final String type = "data";
        runner.createIndex(
                index,
                ImmutableSettings
                        .builder()
                        .put(DynamicRanker.INDEX_DYNARANK_REORDER_SIZE, 100)
                        .put(DynamicRanker.INDEX_DYNARANK_SCRIPT,
                                "searchHits.sort {s1, s2 -> s2.field('counter').value() - s1.field('counter').value()} as org.elasticsearch.search.internal.InternalSearchHit[]")
                        .build());

        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i + "\",\"counter\":" + i + "}");
            assertTrue(indexResponse1.isCreated());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("100", hits.hits()[0].id());
            assertEquals("91", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(50).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("50", hits.hits()[0].id());
            assertEquals("41", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(90).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("10", hits.hits()[0].id());
            assertEquals("1", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(91).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("9", hits.hits()[0].id());
            assertEquals("1", hits.hits()[8].id());
            assertEquals("101", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(95).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("5", hits.hits()[0].id());
            assertEquals("1", hits.hits()[4].id());
            assertEquals("101", hits.hits()[5].id());
            assertEquals("105", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(99).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("1", hits.hits()[0].id());
            assertEquals("101", hits.hits()[1].id());
            assertEquals("109", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(100).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("101", hits.hits()[0].id());
            assertEquals("110", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(
                            QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(0).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("20", hits.hits()[0].id());
            assertEquals("11", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(
                            QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(10).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits());
            assertEquals(10, hits.hits().length);
            assertEquals("10", hits.hits()[0].id());
            assertEquals("1", hits.hits()[9].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(
                            QueryBuilders.rangeQuery("counter").from(0).to(20))
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(11).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(20, hits.getTotalHits());
            assertEquals(9, hits.hits().length);
            assertEquals("9", hits.hits()[0].id());
            assertEquals("1", hits.hits()[8].id());
        }

        {
            final SearchResponse searchResponse = client
                    .prepareSearch("sample")
                    .setQuery(QueryBuilders.rangeQuery("counter"))
                    .addField("counter").addSort("counter", SortOrder.ASC)
                    .setFrom(0).setSize(101).execute().actionGet();
            final SearchHits hits = searchResponse.getHits();
            assertEquals(1000, hits.getTotalHits());
            assertEquals(101, hits.hits().length);
            assertEquals("100", hits.hits()[0].id());
            assertEquals("1", hits.hits()[99].id());
            assertEquals("101", hits.hits()[100].id());
        }
    }
}
