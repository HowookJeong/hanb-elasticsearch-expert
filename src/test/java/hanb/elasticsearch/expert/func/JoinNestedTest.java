package hanb.elasticsearch.expert.func;

import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import java.util.ArrayList;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinNestedTest {
	private static final Logger log = LoggerFactory.getLogger(JoinNestedTest.class);
	
	@Test
	public void testCreateIndex() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		String setting = "";
		String[] mapping = new String[1];

		setting = Operators.readFile("schema/join_nested_settings.json");
		mapping[0] = Operators.readFile("schema/join_nested_mappings.json");
	    
	    try { 
		    client.admin().indices().delete(new DeleteIndexRequest("join_nested")).actionGet();
	    } catch (Exception e) {
	    	
	    } finally { 
	    	
	    }
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("join_nested")
			.setSettings(setting)
			.addMapping("purchase_history", mapping[0])
			.execute()
			.actionGet();
			
		client.close();
	}
	
	@Ignore
	public void testCreateDocument() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		// 데이터 색인은 REST API를 이용해서 색인합니다.
		// hanb-elasticsearch-expert 프로젝트 내 data 아래 들어 있는 파일을 이용해서 등록 합니다.
		// curl -s -XPOST 'http://localhost:9200/join_nested/_bulk' --data-binary @join_nested.json
	}
	
	@Ignore
	public void testNestedQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		/*
		{
		    "query": { 
		        "nested" : {
		            "path" : "buyer_items",
		            "score_mode" : "avg",
		            "query" : {
		                "term": {
		                   "buyer_items.item_id": {
		                      "value": 2
		                   }
		                }
		            }
		        }
		    }
		}
		*/
		String path = "buyer_items";
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("buyer_items.item_name", "자전거");
		NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, termQueryBuilder);
		nestedQueryBuilder.scoreMode("avg");	// default multiply : http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html
		
		String searchResult = Operators.executeQuery(settings, client, nestedQueryBuilder, "join_nested");
		
		log.debug(searchResult);
		
		/*
		{
		    "query" : {
		        "term": {
		           "buyer_login_id": {
		              "value": "atie"
		           }
		        }
		    }
		}
		*/
		
		termQueryBuilder = new TermQueryBuilder("buyer_login_id", "atie");
		searchResult = Operators.executeQuery(settings, client, termQueryBuilder, "join_nested");
		
//		log.debug(searchResult);
	}
	
	@Ignore
	public void testNestedFilter() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		/*
		{
		  "filtered" : {
		    "query" : {
		      "match_all" : { }
		    },
		    "filter" : {
		      "nested" : {
		        "filter" : {
		          "term" : {
		            "buyer_items.item_id" : 2
		          }
		        },
		        "join" : true,
		        "path" : "buyer_items"
		      }
		    }
		  }
		}
		*/
		String path = "buyer_items";
		TermFilterBuilder termFilterBuilder = new TermFilterBuilder("buyer_items.item_id", 2);
		NestedFilterBuilder nestedFilterBuilder = new NestedFilterBuilder(path, termFilterBuilder);
		FilteredQueryBuilder filteredQueryBuilder = new FilteredQueryBuilder(new MatchAllQueryBuilder(), nestedFilterBuilder);
		nestedFilterBuilder.join(true); // 이 기능을 off 할경우 결과가 매칭 되지 않음.

		
//		log.debug(filteredQueryBuilder.toString());
		
		String searchResult = Operators.executeQuery(settings, client, filteredQueryBuilder, "join_nested");
		
		log.debug(searchResult);
	}
}
