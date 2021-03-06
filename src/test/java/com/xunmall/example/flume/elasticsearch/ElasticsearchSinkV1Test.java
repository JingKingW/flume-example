package com.xunmall.example.flume.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/11/5 17:03
 */
public class ElasticsearchSinkV1Test {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchSinkV1Test.class);

    private TransportClient transportClient;

    private String esIndex = "logstash-flume";

    private String type = "flume_kafka_es";

    @Before
    public void init() throws UnknownHostException {
        String esHost = "10.241.122.142";
        Settings settings = Settings.builder().put("cluster.name", "my-es").build();
        transportClient = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), 9300));
    }

    @Test
    public void testInsertToEs() throws IOException {
        String body = "msg234";
        JSONObject json = new JSONObject();
        json.put("message", body);
        IndexResponse response = transportClient.prepareIndex(esIndex, type,"122").setSource(json)
                .get();
        System.out.println("????????????:" + response.getIndex() + "\n??????:" + response.getType() + "\n??????ID:" + response.getId()
                + "\n??????????????????:" + response.status());


    }

    /**
     * ????????????es ??????index type id
     */
    @Test
    public void search() throws UnknownHostException {
        // ????????????????????????
        GetResponse response = transportClient.prepareGet(esIndex, type, "10").get();
        System.out.println(response.getSource());
    }

    /**
     * ????????????
     */
    @Test
    public void insert() throws Exception {

        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("catid", "22")
                .field("classify", 54)
                .field("author", "ssve")
                .field("id", "1")
                .field("title", "???????????????")
                .endObject();
        IndexResponse indexResponse = transportClient.prepareIndex(esIndex, type, "10")
                .setSource(contentBuilder)
                .get();
        System.out.println(indexResponse.status());
    }

    /**
     * ????????????
     */
    @Test
    public void delete() throws Exception {

        DeleteResponse deleteResponse = transportClient.prepareDelete("index3", "user3", "10").get();

        System.out.println(deleteResponse.status());
    }

    /**
     * ????????????
     */
    @Test
    public void update() throws Exception {
        UpdateRequest request = new UpdateRequest();
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "555")
                .endObject();
        request.index("index3")
                .type("user3")
                .id("10")
                .doc(contentBuilder);
        UpdateResponse updateResponse = transportClient.update(request).get();

        System.out.println(updateResponse.status());
    }

    /**
     * upsert?????? ?????????????????????????????????  ??????????????????  ??????????????????????????? ??????????????????
     */
    @Test
    public void upsert() throws IOException, ExecutionException, InterruptedException {

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("catid", "22")
                .field("classify", 54)
                .field("author", "zhang")
                .field("id", "10")
                .field("title", "???????????????")
                .endObject();
        UpdateRequest request = new UpdateRequest();
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index("index3")
                .type("user3")
                .id("11")
                .source(builder);
        request.index("index3")
                .type("user3")
                .id("11")
                .doc(new XContentFactory().jsonBuilder()
                        .startObject()
                        .field("id", "i love you")
                        .endObject()
                ).upsert(indexRequest);
        UpdateResponse updateResponse = transportClient.update(request).get();

        System.out.println(updateResponse.status());

    }

    /**
     * ????????????  multiGet
     */
    @Test
    public void multiGet() throws Exception {

        MultiGetRequest request = new MultiGetRequest();
        request.add("index3", "user3", "11");
        request.add("index3", "user3", "10");
        request.add("index3", "user3", "13");
        request.add("index3", "user3", "14");
        MultiGetResponse multiGetItemResponses = transportClient.multiGet(request).get();

        for (MultiGetItemResponse response : multiGetItemResponses) {
            System.out.println(response.getResponse().getSourceAsString());
        }
    }

    /**
     * bulk??????????????????????????????
     */
    @Test
    public void bulk() throws Exception {

        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        IndexRequest request = new IndexRequest();
        request.index("index3")
                .type("user3")
                .id("13")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("catid", "85")
                        .field("classify", "85")
                        .field("author", "????????????")
                        .field("id", "??????")
                        .field("title", "??????")
                        .endObject());

        IndexRequest request1 = new IndexRequest();
        request1.index("index3")
                .type("user3")
                .id("14")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("catid", "55")
                        .field("classify", "85")
                        .field("author", "????????????")
                        .field("id", "??????")
                        .field("title", "??????")
                        .endObject());
        bulkRequestBuilder.add(request1);
        bulkRequestBuilder.add(request);
        BulkResponse bulkItemResponses = bulkRequestBuilder.get();
        System.out.println(bulkItemResponses.status());

    }

    /**
     * ??????query??????  match_all ????????????
     */
    @Test
    public void query() throws Exception {
        MatchAllQueryBuilder builder = QueryBuilders.matchAllQuery();
        SearchRequestBuilder index3 = transportClient.prepareSearch("index3")
                .setQuery(builder)
                .setSize(5);
        SearchResponse searchResponse = index3.get();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ??????query??????  match ??????
     */
    @Test
    public void matchQuery() throws Exception {
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("title", "???");
        SearchRequestBuilder index = transportClient.prepareSearch("index3").setQuery(matchQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit : hits
                ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ??????query??????  mutilMatch ??????
     */
    @Test
    public void mutilMatchQuery() throws Exception {
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery("???????????????", "author", "title");
        SearchRequestBuilder index = transportClient.prepareSearch("index3").setQuery(multiMatchQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit : hits
                ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ??????query?????? termheterms??????
     */
    @Test
    public void termsMatchQuery() throws Exception {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("title", "?????????", "??????");
        SearchRequestBuilder index = transportClient.prepareSearch("index3").setQuery(termsQueryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit : hits
                ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ??????query??????  ??????  ????????? ?????? ????????????
     */
    @Test
    public void query1() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1", "2", "4");
        SearchRequestBuilder index = transportClient.prepareSearch("index3").setQuery(queryBuilder).setSize(10);
        SearchHits hits = index.get().getHits();
        for (SearchHit hit : hits
                ) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     */
    @Test
    public void aggregation() throws Exception {
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("max").field("id");

        SearchResponse index3 = transportClient.prepareSearch("index3").addAggregation(aggregationBuilder).get();

        Max max = index3.getAggregations().get("max");

        System.out.println(max.getValue());
    }

    /**
     * queryString
     */
    @Test
    public void queryString() throws Exception {
        // + ??????????????? -????????????
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("+????????? -sss");
        SearchRequestBuilder index3 = transportClient.prepareSearch("index3")
                .setQuery(queryBuilder)
                .setSize(10);
        SearchResponse searchResponse = index3.get();
        for (SearchHit hit : searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

    /**
     * ????????????
     */
    @Test
    public void boolQuery() throws Exception {

        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("author", "?????????"))
                .mustNot(QueryBuilders.matchQuery("title", "?????????"))
                .should(QueryBuilders.matchQuery("title", "??????"))
                .filter(QueryBuilders.rangeQuery("id").gte("1"));

        SearchRequestBuilder index3 = transportClient.prepareSearch("index3")
                .setQuery(queryBuilder)
                .setSize(10);
        SearchResponse searchResponse = index3.get();
        for (SearchHit hit : searchResponse.getHits()) {
            System.out.println(hit.getSourceAsString());
        }
    }

}