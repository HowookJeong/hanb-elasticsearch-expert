package hanb.elasticsearch.expert.func;

import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import java.io.BufferedReader;
import java.io.FileReader;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramBuilder;
import org.elasticsearch.search.aggregations.bucket.missing.MissingBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedBuilder;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNestedBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.date.DateRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.geodistance.GeoDistanceBuilder;
import org.elasticsearch.search.aggregations.bucket.significant.SignificantTermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStatsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregationTest {
	private static final Logger log = LoggerFactory.getLogger(AggregationTest.class);
	/*
	numeric field : min, max, sum, avg, stats, extended_stats, percentiles(분포도), range, histogram
	string or numeric field : value_count, cardinality(distinct values/terms), missing(set value null), terms, significant terms
	date field : date range, date histogram
	ipv4 field : ipv4 range
	geo point field : geo histogram, geohash grid
	*/
	@Ignore
	public void testCreateIndex() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		String setting = "";
		String[] mapping = new String[1];

		// percolate index
		setting = Operators.readFile("schema/aggregations_settings.json");
		mapping[0] = Operators.readFile("schema/aggregations_mappings.json");
	    
	    try { 
		    client.admin().indices().delete(new DeleteIndexRequest("aggregations")).actionGet();
	    } catch (Exception e) {
	    	
	    } finally { 
	    	
	    }
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("aggregations")
			.setSettings(setting)
			.addMapping("transactions", mapping[0])
			.execute()
			.actionGet();
			
		client.close();
	}
	
	@Ignore
	public void testAddIndexing() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		IndexRequestBuilder requestBuilder;
		IndexResponse response;

		requestBuilder = client.prepareIndex("aggregations", "transactions");
		
		BufferedReader br = new BufferedReader(new FileReader("/Users/hwjeong/Documents/workspace/eclipse-j2ee/hanb-elasticsearch-expert/data/aggregations_bulk.json"));
		
	    try {
	        String line = "";

	        while ((line = br.readLine()) != null) {
	        	response = requestBuilder.setSource(line)
	        		.execute()
	        		.actionGet();
	        	log.debug("{}", response.getId());
	        }
	    } finally {
	        br.close();
	    }
	}
	
	@Ignore
	public void testBulkIndexing() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		BulkRequestBuilder bulkRequest;
		BulkResponse bulkResponse;
		BufferedReader br = new BufferedReader(new FileReader("/Users/hwjeong/Documents/workspace/eclipse-j2ee/hanb-elasticsearch-expert/data/aggregations_bulk.json"));

	    try {
	        String doc = "";
	        bulkRequest = client.prepareBulk();

	        while ((doc = br.readLine()) != null) {
	        	bulkRequest.add (
					client.prepareIndex("aggregations", "transactions")
						.setOperationThreaded(false)
						.setSource(doc)
						.setReplicationType(ReplicationType.ASYNC)
						.setConsistencyLevel(WriteConsistencyLevel.QUORUM)
						.setRefresh(false)
				);
	        }
	        
	        bulkResponse = bulkRequest.execute().actionGet();
	        log.debug("{}", bulkResponse.getTookInMillis());
	    } finally {
	        br.close();
	    }
	    
	    client.close();
	}
	
	@Ignore
	public void testAggsMin() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		MinBuilder aggsBuilder = AggregationBuilders.min("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsMax() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		MaxBuilder aggsBuilder = AggregationBuilders.max("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsSum() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		SumBuilder aggsBuilder = AggregationBuilders.sum("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsAvg() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		AvgBuilder aggsBuilder = AggregationBuilders.avg("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsStats() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		StatsBuilder aggsBuilder = AggregationBuilders.stats("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsExtendedStats() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		ExtendedStatsBuilder aggsBuilder = AggregationBuilders.extendedStats("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// 즉 지정한 field 에서 추출된 value 가 있을 경우 그 추출된 value count 를 구한다.
	public void testAggsValueCount() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		ValueCountBuilder aggsBuilder = AggregationBuilders.count("aggs_result");
		aggsBuilder.field("buyer_country"); // 20개 로 null value가 하나 존재 함.
//		aggsBuilder.field("payment_price");	// 21개 
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsPercentiles() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		PercentilesBuilder aggsBuilder = AggregationBuilders.percentiles("aggs_result");
		aggsBuilder.field("payment_price");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsCardinality() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		CardinalityBuilder aggsBuilder = AggregationBuilders.cardinality("aggs_result");
		aggsBuilder.field("item_code");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");
		
		log.debug(searchResult);
		
		aggsBuilder = AggregationBuilders.cardinality("aggs_result");
		aggsBuilder.field("buyer_country");
		
		searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// global aggregation 의 경우 무조건 전체 데이터에 대해서 작업을 수행 한다.
	public void testAggsGlobal() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		GlobalBuilder aggsBuilder = AggregationBuilders.global("aggs_result");
		AvgBuilder subAggsBuilder = AggregationBuilders.avg("sub_aggs_result");
		subAggsBuilder.field("payment_price");
		
		aggsBuilder.subAggregation(subAggsBuilder);

//		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");
		String searchResult = Operators.executeAggregation(settings, client, new TermQueryBuilder("buyer_country", "KR"), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// aggregation 에서의 filter 컨셉은 기본적으로 having 절과 유사하다고 이해 하면 쉽다.
	// 기본적일 검색 질의 결과에 대해서 aggregation 에 대한 filter 조건을 추가 하여 해당 filter 조건에 대한 결과의 aggregation 결과를 리턴 하도록 한다.
	// filtered : 검색 결과와 aggregation 결과에 동시에 영향
	// filter : 검색 결과를 바탕으로 aggregation 결과에만 영향
	// post_filter : 검색 결과에만 영향을 미치며 aggregation 결과에는 영향을 주지 않는다. (global aggregation 과 유사함.)
	public void testAggsFilter() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		RangeFilterBuilder filterBuilder = FilterBuilders.rangeFilter("payment_time").from("20140619163000");	// included equals true
//		RangeFilterBuilder filterBuilder = FilterBuilders.rangeFilter("payment_time").gt("20140619163000");
		FilterAggregationBuilder aggsBuilder = AggregationBuilders.filter("aggs_result");
		AvgBuilder subAggsBuilder = AggregationBuilders.avg("sub_aggs_result");
		subAggsBuilder.field("payment_price");
		
		aggsBuilder.filter(filterBuilder);
		aggsBuilder.subAggregation(subAggsBuilder);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// missing document 를 생성하기 위해서는 field 값이 null 이거나 field 자체가 없어야 한다.
	public void testAggsMissing() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		MissingBuilder aggsBuilder = AggregationBuilders.missing("aggs_result");
		aggsBuilder.field("buyer_country");
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsNested() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		NestedBuilder aggsBuilder = AggregationBuilders.nested("aggs_result");
		AvgBuilder subAggsBuilder = AggregationBuilders.avg("sub_aggs_result");
		subAggsBuilder.field("buyer_items.item_price");
		
		aggsBuilder.path("buyer_items");
		aggsBuilder.subAggregation(subAggsBuilder);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "join_nested", "purchase_history");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// revers nested 는 nested 문서를 포함하고 있는 parent 문서에 대한 aggregation 분석 결과를 함께 넘겨 준다.
	public void testAggsReverseNested() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		NestedBuilder aggsBuilder = AggregationBuilders.nested("aggs_result");
		TermsBuilder subAggsBuilder = AggregationBuilders.terms("sub_aggs_result");
		ReverseNestedBuilder reverseAggsBuilder = AggregationBuilders.reverseNested("rev_aggs_result");
		TermsBuilder revSubAggsBuilder = AggregationBuilders.terms("rev_sub_aggs_result");
		
		revSubAggsBuilder.field("buyer_login_id");
		reverseAggsBuilder.subAggregation(revSubAggsBuilder);
		subAggsBuilder.field("buyer_items.item_id");
		subAggsBuilder.subAggregation(reverseAggsBuilder);
		aggsBuilder.path("buyer_items");
		aggsBuilder.subAggregation(subAggsBuilder);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "join_nested", "purchase_history");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Test
	// shard_size 는 size 보다 작을 수 없다. (작다면 size 와 같은 값으로 적용된다.)
	public void testAggsTerms() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermsBuilder aggsBuilder = AggregationBuilders.terms("aggs_result");
		aggsBuilder.field("buyer_country").size(0).order(Terms.Order.count(false));
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}

	@Ignore
	// significant aggregation 은 검색 결과에 대한 특이 점에 대한 분석을 해준다.
	// 아래는 전체 구매 상품 중 상품명에 대한 구매 성별 특이점을 분석해 준 내용이다.
	public void testAggsSignificant() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = QueryBuilders.termQuery("item_name", "자전거");
		SignificantTermsBuilder aggsBuilder = AggregationBuilders.significantTerms("aggs_result");
		aggsBuilder.field("item_category");
		
		String searchResult = Operators.executeAggregation(settings, client, queryBuilder, aggsBuilder, "aggregations", "transactions");
		log.debug(searchResult);
		
		TermsBuilder termsBuilder = AggregationBuilders.terms("aggs_result");
		termsBuilder.field("item_name");
		SignificantTermsBuilder subAggsBuilder = AggregationBuilders.significantTerms("sub_aggs_result");
		subAggsBuilder.field("buyer_gender");
		
		termsBuilder.subAggregation(subAggsBuilder);
		
		searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), termsBuilder, "aggregations", "transactions");
//		searchResult = Operators.executeAggregation(settings, client, queryBuilder, termsBuilder, "aggregations", "transactions");
		log.debug(searchResult);

		client.close();
	}
	
	@Ignore
	// range aggregation 은 number type의 field 만 사용 가능 함.
	// range 에서 사용하는 from 값은 포함 하지만 to 값은 포함하지 않는다.
	public void testAggsRange() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		RangeBuilder aggsBuilder = AggregationBuilders.range("aggs_result");
//		aggsBuilder.field("payment_price").addRange(0, 250000).addRange(250001, 500000).addRange(500001, 1000000);
		aggsBuilder.field("payment_price").addUnboundedTo(250000).addUnboundedTo(500000).addUnboundedTo(1000000);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// range 에서 사용하는 from 값은 포함 하지만 to 값은 포함하지 않는다.
	public void testAggsDateRange() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		DateRangeBuilder aggsBuilder = AggregationBuilders.dateRange("aggs_result");
		aggsBuilder.field("payment_time")
			.format("yyyyMMddHHmmss")
			.addRange("target", "20140604163000", "20140604163001")
			.addRange("201406H1", "20140531235959", "20140616000000")
			.addRange("201406H2", "20140615235959", "20140701000000");
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsHistogram() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		HistogramBuilder aggsBuilder = AggregationBuilders.histogram("aggs_result");
		aggsBuilder.field("payment_price")
			.interval(5000);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testAggsDateHistogram() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		DateHistogramBuilder aggsBuilder = AggregationBuilders.dateHistogram("aggs_result");
		aggsBuilder.field("payment_time")
			.interval(Interval.DAY);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
	}
	
	@Ignore
	// arc -> sloppy_arc -> plane 순으로 속도가 빠르나 반대로 정확도는 떨어진다.
	// default KM 로 거리 연산을 한다.
	public void testAggsGeoDistance() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		GeoDistanceBuilder aggsBuilder = AggregationBuilders.geoDistance("aggs_result");
		aggsBuilder.field("buyer_location.location")
			.point("35.907757,127.76692200000002")
			.distanceType(GeoDistance.ARC)
			.unit(DistanceUnit.KILOMETERS)
			.addUnboundedTo(1000);
		
		String searchResult = Operators.executeAggregation(settings, client, new MatchAllQueryBuilder(), aggsBuilder, "aggregations", "transactions");

		client.close();
		
		log.debug(searchResult);
		
		MatchAllQueryBuilder 
			matchAllQueryBuilder = new MatchAllQueryBuilder();

	}
}
