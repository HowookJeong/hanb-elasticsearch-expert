package hanb.elasticsearch.expert.func;

import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.HasChildQueryBuilder;
import org.elasticsearch.index.query.HasParentFilterBuilder;
import org.elasticsearch.index.query.HasParentQueryBuilder;
import org.elasticsearch.index.query.TermFilterBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * 기본적으로 1:N 구조의 relation 을 생성하기 위해서는 parent type 이 1 관계를 가져야 하고, child type 이 N 관계를 가져야 한다.
 * parent type 에서 조건 검색을 통해 나온 결과를 바탕으로 child type 에서 결과를 획득하게 된다.
 * 	case 1) 
 * 		has_parent 로 질의를 실행
 * 		child type 에서 결과 리턴
 * 			리턴된 결과에는 parent type 의 내용은 포함되지 않는다.
 * 	case 2)
 * 		has_child 로 질의를 실행
 * 		parent type 에서 결과 리턴
 * 			리턴된 결과에는 child type 의 내용은 포함되지 않는다.
 */
public class JoinParentChildTest {
	private static final Logger log = LoggerFactory.getLogger(JoinParentChildTest.class);
	
	@Test
	public void testCreateIndexHasParent() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		String setting = "";
		String[] mapping = new String[2];

		setting = Operators.readFile("schema/join_parent_child_settings.json");
		mapping[0] = Operators.readFile("schema/join_parent_child_mappings_parent1.json");
		mapping[1] = Operators.readFile("schema/join_parent_child_mappings_childN.json");
	    
	    try { 
		    client.admin().indices().delete(new DeleteIndexRequest("join_parent_child1n")).actionGet();
	    } catch (Exception e) {
	    } finally {
	    }
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("join_parent_child1n")
			.setSettings(setting)
			.addMapping("buyer", mapping[0])
			.addMapping("buyer_item", mapping[1])
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
		// curl -s -XPOST 'http://localhost:9200/join_parent_child1n/_bulk' --data-binary @join_parent_child1N.json
	}
	
	@Ignore
	public void testHashParentQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});

		/*
		{
		    "query": {
		        "has_parent": {
		            "parent_type": "buyer",
		            "query": {
		                "term" : {
		                    "buyer_login_id" : "atie"
		                }
		            }
		        }
		    }
		}
		*/
		
		String parentType = "buyer";
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("buyer_login_id", "atie");
		HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(parentType, termQueryBuilder);
		String searchResult = Operators.executeQuery(settings, client, hasParentQueryBuilder, "join_parent_child1n");

		log.debug(searchResult);
	}
	
	@Ignore
	// child type 에 질의를 하여 매칭된 결과의 parent 결과를 리턴.
	// query 이외 filter 를 사용할 경우 query 를 모두 filter 로 변경 한 후 질의 하도록 한다.
	// query : { "query" : { "had_xxxx" : { ...., "query" : {}}}}
	// filter : { "filter" : { "had_xxxx" : { ...., "filter" : {}}}}
	public void testHasChildQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});

		/*
		{
		    "query": {
		        "has_child": {
		            "type": "buyer_item",
		            "query": {
		                "term" : {
		                    "item_id" : 1
		                }
		            }
		        }
		    }
		}
		*/
		
		String childType = "buyer_item";
		TermQueryBuilder termQueryBuilder = new TermQueryBuilder("item_id", 1);
		HasChildQueryBuilder hasChildQueryBuilder = new HasChildQueryBuilder(childType, termQueryBuilder);
		String searchResult = Operators.executeQuery(settings, client, hasChildQueryBuilder, "join_parent_child1n");

		log.debug(searchResult);
	}
	
	@Ignore
	public void testHashParentFilter() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});

		/*
		{
		    "filter": {
		        "has_parent": {
		            "parent_type": "buyer",
		            "filter": {
		                "term" : {
		                    "buyer_login_id" : "atie"
		                }
		            }
		        }
		    }
		}
		*/
		
		String parentType = "buyer";
		TermFilterBuilder termFilterBuilder = new TermFilterBuilder("buyer_login_id", "atie");
		HasParentFilterBuilder hasParentFilterBuilder = new HasParentFilterBuilder(parentType, termFilterBuilder);
		String searchResult = Operators.executeFilter(settings, client, hasParentFilterBuilder, "join_parent_child1n");

		log.debug(searchResult);
	}
}
