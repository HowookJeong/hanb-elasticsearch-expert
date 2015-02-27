package hanb.elasticsearch.expert.func;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RiverJdbcTest {
	private static final Logger log = LoggerFactory.getLogger(RiverJdbcTest.class);
	
	@Test
	public void testCreateIndex() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		String setting = "";
		String[] mapping = new String[1];

		setting = Operators.readFile("schema/river_jdbc_mysql_settings.json");
		mapping[0] = Operators.readFile("schema/river_jdbc_mysql_mappings.json");
	    
	    try { 
		    client.admin().indices().delete(new DeleteIndexRequest("river_jdbc_mysql")).actionGet();
	    } catch (Exception e) {
	    } finally { 
	    }
	    
		CreateIndexResponse createIndexResponse = client.admin().indices()
			.prepareCreate("river_jdbc_mysql")
			.setSettings(setting)
			.addMapping("tbl_item_increment", mapping[0])
			.execute()
			.actionGet();
			
		client.close();
	}
	
	@Test
	public void testAddJdbcRiver() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		IndexRequestBuilder requestBuilder;
		IndexResponse response;
		/*
		curl -XPUT 'localhost:9200/_river/jdbc_mysql_fetch/_meta' -d '{
		    "type" : "jdbc",
		    "jdbc" : {
		        "url" : "jdbc:mysql://localhost:3306/test",
		        "user" : "root",
		        "password" : "",
		        "sql" : [
		        	{
		        		"statement" : "SELECT item_id, item_code, item_name, price, regdate FROM TBL_ITEM_INCREMENT WHERE fetch_flag = ? LIMIT 0, 2",
		        		"parameter" : ["F"],
		        		"callable" : false
		        	}
		        ],
		        "index" : "river_jdbc_mysql",
		        "type" : "tbl_item_increment",
		        "schedule" : "0 0-59 0-23 ? * *"
		    }
		}'
		*/
		XContentBuilder json = jsonBuilder().startObject();
			json.field("type", "jdbc");
			json.field("jdbc");
			json.startObject();
				json.field("url",  "jdbc:mysql://localhost:3306/test");
				json.field("user", "root");
				json.field("password", "");
				json.field("sql");
				json.startArray();
					json.startObject();
						json.field("statement", "SELECT item_id, item_code, item_name, price, regdate FROM TBL_ITEM_INCREMENT WHERE fetch_flag = ? LIMIT 0, 2");
						json.field("parameter");
						json.startArray();
							json.value("F");
						json.endArray();
						json.field("callable", false);
					json.endObject();
				json.endArray();
				json.field("index", "river_jdbc_mysql");
				json.field("type", "tbl_item_increment");
				json.field("schedule", "0 0-59 0-23 ? * *");
				
			json.endObject();
		json.endObject();
		
		log.debug(json.bytes().toUtf8());
	
		requestBuilder = client.prepareIndex("_river", "jdbc_mysql_fetch");
		response = requestBuilder.setId("_meta")
				.setSource(json)
				.execute()
				.actionGet();
		
		/*
		curl -XPUT 'localhost:9200/_river/jdbc_mysql_update/_meta' -d '{
		    "type" : "jdbc",
		    "jdbc" : {
		        "url" : "jdbc:mysql://localhost:3306/test",
		        "user" : "root",
		        "password" : "",
		        "sql" : [
		        	{
		        		"statement" : "UPDATE (SELECT * FROM TBL_ITEM_INCREMENT WHERE fetch_flag=? LIMIT 0,2) src, TBL_ITEM_INCREMENT dest SET dest.fetch_flag=? WHERE dest.item_id=tbl.item_id",
		        		"parameter" : ["F", "T"],
		        		"callable" : false,
		        		"write" : true
		        	}
		        ],
		        "index" : "river_jdbc_mysql",
		        "type" : "tbl_item_increment",
		        "schedule" : "0 0-59 0-23 ? * *"
		    }
		}'
		*/
		json = jsonBuilder().startObject();
			json.field("type", "jdbc");
			json.field("jdbc");
			json.startObject();
				json.field("url",  "jdbc:mysql://localhost:3306/test");
				json.field("user", "root");
				json.field("password", "");
				json.field("sql");
				json.startArray();
					json.startObject();
						json.field("statement", "UPDATE (SELECT * FROM TBL_ITEM_INCREMENT WHERE fetch_flag=? LIMIT 0,2) src, TBL_ITEM_INCREMENT dest SET dest.fetch_flag=? WHERE dest.item_id=src.item_id");
						json.field("parameter");
						json.startArray();
							json.value("F");
							json.value("T");
						json.endArray();
						json.field("callable", false);
						json.field("write", true);
					json.endObject();
				json.endArray();
				json.field("index", "river_jdbc_mysql");
				json.field("type", "tbl_item_increment");
				json.field("schedule", "0 0-59 0-23 ? * *");
				
			json.endObject();
		json.endObject();
		
		log.debug(json.bytes().toUtf8());
	
		requestBuilder = client.prepareIndex("_river", "jdbc_mysql_update");
		response = requestBuilder.setId("_meta")
				.setSource(json)
				.execute()
				.actionGet();
	}
}
