package hanb.elasticsearch.expert.func;


import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.percolate.PercolateRequestBuilder;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateResponse.Match;
import org.elasticsearch.action.percolate.PercolateSourceBuilder.DocBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PercolateTest {
	// percolator 는 search in reverse 라는 관점에서 접근해야 한다.
	// 즉, 색인을 목적으로 하기 보다, 문서 모니터링을 목적으로 한다고 이해해야 한다.
	// 먼저 모니터링(검색)할 쿼리를 등록해 두고 _percolate 를 통해 문서를 요청해서 매칭된 조건을 찾는다고 이해 하면 된다.
	// use case 로는 실시간으로 데이터에 대한 변화를 감지 하기 위한 서비스에서 활용하는 것이 좋다. (광고, 경매, 자동분류... )
	// Step 1. 문서를 저장 또는 색인 하기 위해 색인 요청을 한다. (auction)
	// Step 2. 색인 요청 후 모니터링을 위한 percolate 요청을 한다. (alert)
	// Step 3. 2번 결과에 따른 action 을 수행 한다.
	// 	step 1, 2순서를 바꿔서 할 경우 percolate 를 확인 후 action 에 따른 색인을 할 수도 있다.
	// Reversed search
	// 		storing queries instead of data.
	// 		querying with data instead of queries.
	// CPU intensive
	// co-exist in the same index
	// dedicated percolate index
	
	// ./create_index.sh localhost:9200 auction_bidding_log percolate.json
	// ./create_index.sh localhost:9200 auction_bidding_percolate percolate.json
	private static final Logger log = LoggerFactory.getLogger(PercolateTest.class);
	
	@Ignore
	public void testCreateIndex() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		String setting = "";
		String[] mapping = new String[4];

		// percolate index
		setting = Operators.readFile("schema/percolate_settings.json");
		mapping[0] = Operators.readFile("schema/percolate_mappings_special_order.json");
		mapping[1] = Operators.readFile("schema/percolate_mappings_over_price.json");
		mapping[2] = Operators.readFile("schema/percolate_mappings_reserved_order.json");
		mapping[3] = Operators.readFile("schema/percolate_mappings_auction_1.json");

	    try { 
		    client.admin().indices().delete(new DeleteIndexRequest("auction_bidding_log")).actionGet();
		    client.admin().indices().delete(new DeleteIndexRequest("auction_bidding_percolate")).actionGet();
	    } catch (Exception e) {
	    	
	    } finally { 
	    	
	    }
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("auction_bidding_log")
			.setSettings(setting)
			.addMapping("special_order", mapping[0])
			.addMapping("over_price", mapping[1])
			.addMapping("reserved_order", mapping[2])
			.addMapping("auction_1", mapping[3])
			.execute()
			.actionGet();
		
		// percolate alert index
		// percolate alert 용 index 생성 시 doctype 선언 즉 mapping 정보를 포함한 doctype 이 없을 경우 정상 동작 하지 않는 버그 존재.
		createIndexResponse = client.admin().indices()
			.prepareCreate("auction_bidding_percolate")
			.setSettings(setting)
			// 1.3.4 에서 버그 수정되어 empty mapping 추가 하지 않아도 됨.
//			.addMapping("auction", mapping[4])
			.execute()
			.actionGet();
			
		client.close();
	}
	
	@Ignore
	public void testAddPecolateQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		/*
		{
			"query" : {
				"match" : {
					"bidding_keyword" : {
						"query" : "special order",
						"operator" : "and"
					}
				}
			}
		}
		 */
		// case 1
		QueryBuilder queryBuilder = QueryBuilders.matchQuery("bidding_keyword", "special order").operator(Operator.AND);
		XContentBuilder json = jsonBuilder().startObject();
			json.field("query", queryBuilder);
		json.endObject();
		log.debug("{}", json.bytes().toUtf8());
		
		// case 2
		MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("bidding_keyword", "special order");
		matchQueryBuilder.operator(Operator.AND);
		String source = "{\"query\" : " + matchQueryBuilder.toString() + "}";
		log.debug("{}", source);
		
		client.prepareIndex("auction_bidding_percolate", ".percolator", "special_order")
//			.setSource(source)
			.setSource(json)
			.execute()
			.actionGet();
		
		/*
		{
			"query" : {
				"match" : {
					"bidding_keyword" : {
						"query" : "over price",
						"operator" : "and"
					}
				}
			}
		}
		*/
		// case 1
		queryBuilder = QueryBuilders.matchQuery("bidding_keyword", "over price").operator(Operator.AND);
		json = jsonBuilder().startObject();
			json.field("query", queryBuilder);
		json.endObject();
		log.debug("{}", json.bytes().toUtf8());
		
		client.prepareIndex("auction_bidding_percolate", ".percolator", "over_price")
			.setSource(json)
			.execute()
			.actionGet();
		/*
		{
			"query" : {
				"match" : {
					"bidding_keyword" : {
						"query" : "reserved order",
						"operator" : "and"
					}
				}
			}
		}
		*/
		// case 1
		queryBuilder = QueryBuilders.matchQuery("bidding_keyword", "reserved order").operator(Operator.AND);
		json = jsonBuilder().startObject();
			json.field("query", queryBuilder);
		json.endObject();
		log.debug("{}", json.bytes().toUtf8());
		
		client.prepareIndex("auction_bidding_percolate", ".percolator", "reserved_order")
			.setSource(json)
			.execute()
			.actionGet();
		
		/*
		{
		    "query": {
		        "bool": {
		            "must": [
		                {
		                    "term": {
		                        "auction_id": 1
		                    }
		                },
		                {
		                    "range": {
		                        "bidding_price": {
		                            "gt": 10000000
		                        }
		                    }
		                }
		            ]
		        }
		    }
		}
		*/
		// case 1
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("auction_id", 1);
		RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("bidding_price");
		rangeQueryBuilder.gt(10000000);
		queryBuilder = QueryBuilders.boolQuery().must(rangeQueryBuilder).must(termQueryBuilder);
		
		json = jsonBuilder().startObject();
			json.field("query", queryBuilder);
		json.endObject();
		log.debug("{}", json.bytes().toUtf8());
		
		client.prepareIndex("auction_bidding_percolate", ".percolator", "auction_1")
			.setSource(json)
			.execute()
			.actionGet();
	}
	
	@Test
	public void testPercolate2AddDocumentCase1() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		// check percolate
		PercolateRequestBuilder precolateRequestBuilder = new PercolateRequestBuilder(client);
		// 가상의 경매 입찰 문서를 생성한다.
		DocBuilder docBuilder = new DocBuilder();
		XContentBuilder jsonDoc = jsonBuilder().startObject()
				.field("auction_id", 1)
				.field("bidding_id", 1)
	            .field("bidding_keyword",  "special order")
	            .field("bidding_price", 200000000)
	        .endObject();
		
		docBuilder.setDoc(jsonDoc);
		
		// percolator request를 보낸다.
		PercolateResponse percolateResponse = precolateRequestBuilder.setIndices("auction_bidding_percolate")
				.setDocumentType(".percolator")
				.setPercolateDoc(docBuilder)
				.execute()
				.actionGet();
		
		// add document
		// if auction_id == 1 and bidding_price > 10000000, auction will be closed.
		// or logging.....
		Match[] matches = percolateResponse.getMatches();
		int size = matches.length;
		
		log.debug("{}", size);
		
		for ( int i=0; i<size; i++ ) {
			// 매칭된 alert use case 의 INDEX와 ID 를 가져 온다.
//			log.debug("{}", matches[i].getIndex().string());
//			log.debug("{}", matches[i].getId().string());
			
			String docType = matches[i].getId().string();
			
			if ( "auction_1".equalsIgnoreCase(docType) ) {
				IndexRequestBuilder requestBuilder;
				IndexResponse response;
				
				requestBuilder = client.prepareIndex("auction_bidding_log", docType);
				response = requestBuilder
						.setSource(jsonDoc)
						.execute()
						.actionGet();
				
				log.debug("{}", response.getId());
			}
			
			if ( "special_order".equalsIgnoreCase(docType) ) {
				IndexRequestBuilder requestBuilder;
				IndexResponse response;
				
				requestBuilder = client.prepareIndex("auction_bidding_log", docType);
				response = requestBuilder
						.setSource(jsonDoc)
						.execute()
						.actionGet();
				
				log.debug("{}", response.getId());
			}
		}
	}
	
	@Test
	public void testPercolate2AddDocumentCase2() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		// check percolate
		XContentBuilder doc = jsonBuilder().startObject()
			.startObject("doc")
				.field("bidding_id", 2)
	            .field("auction_id", 1)
	            .field("bidding_price", 2000000)
	            .field("bidding_keyword",  "reserved order")
			.endObject()
		.endObject();
		
		log.debug("{}", doc.bytes().toUtf8());
		
		PercolateResponse percolateResponse = client.preparePercolate().setIndices("auction_bidding_percolate")
			.setDocumentType(".percolator")
			.setSource(doc)
			.execute()
			.actionGet();
		
		// add document
		// if auction_id == 1 and bidding_price > 10000000, auction will be closed.
		// or logging.....
		Match[] matches = percolateResponse.getMatches();
		int size = matches.length;
		
		XContentBuilder source = jsonBuilder().startObject()
				.field("bidding_id", 2)
	            .field("auction_id", 1)
	            .field("bidding_price", 2000000)
	            .field("bidding_keyword",  "reserved order")
	        .endObject();
			
		
		for ( int i=0; i<size; i++ ) {
			// 매칭된 alert use case 의 INDEX와 ID 를 가져 온다.
			log.debug("{}", matches[i].getIndex().string());
			log.debug("{}", matches[i].getId().string());
			
			String docType = matches[i].getId().string();
			
			if ( "reserved_order".equalsIgnoreCase(docType) ) {
				IndexRequestBuilder requestBuilder;
				IndexResponse response;
				
				requestBuilder = client.prepareIndex("auction_bidding_log", docType);
				response = requestBuilder
						.setSource(source)
						.execute()
						.actionGet();
				
				log.debug("{}", response.getId());
			}
		}
	}
	
	@Test
	public void testAddDocument2Percolate() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		// add document
		XContentBuilder source = jsonBuilder().startObject()
			.field("bidding_id", 3)
            .field("auction_id", 1)
            .field("bidding_price", 99999999)
            .field("bidding_keyword",  "over price")
        .endObject();
		
		IndexRequestBuilder requestBuilder;
		IndexResponse response;
		
		requestBuilder = client.prepareIndex("auction_bidding_log", "over_price");
		response = requestBuilder
				.setSource(source)
				.execute()
				.actionGet();
		
		log.debug("{}", response.getId());
		
		// check percolate
		XContentBuilder doc = jsonBuilder().startObject()
			.startObject("doc")
				.field("bidding_id", 3)
	            .field("auction_id", 1)
	            .field("bidding_price", 99999999)
	            .field("bidding_keyword",  "over price")
			.endObject()
		.endObject();
		
		log.debug("{}", doc.bytes().toUtf8());
		
		PercolateResponse percolateResponse = client.preparePercolate().setIndices("auction_bidding_percolate")
			.setDocumentType(".percolator")
			.setSource(doc)
			.execute()
			.actionGet();
		
		// add document
		// if auction_id == 1 and bidding_price > 10000000, auction will be closed.
		// or logging.....
		Match[] matches = percolateResponse.getMatches();
		int size = matches.length;
		
		for ( int i=0; i<size; i++ ) {
			// 매칭된 alert use case 의 INDEX와 ID 를 가져 온다.
			log.debug("{}", matches[i].getIndex().string());
			log.debug("{}", matches[i].getId().string());
			
			String docType = matches[i].getId().string();
			
			if ( "over_price".equalsIgnoreCase(docType) ) {
				log.debug("Send EMAIL or SMS");
				break;
			}
		}
	}
}
