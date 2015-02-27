package hanb.elasticsearch.expert.common;

import java.io.BufferedReader;
import java.io.FileReader;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

public class Operators {
	/**
	 * file reader
	 * 
	 * @param file		input file path
	 * @return
	 * @throws Exception
	 */
	public static String readFile(String file) throws Exception {
		String ret = "";
		BufferedReader br = new BufferedReader(new FileReader(file));
		
	    try {
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();

	        while (line != null) {
	            sb.append(line);
	            line = br.readLine();
	        }
	        
	        ret = sb.toString();
	    } finally {
	        br.close();
	    }
	    
	    return ret;
	}
	
	public static String executeQuery(Settings settings, Client client, QueryBuilder queryBuilder, String indice) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setQuery(queryBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeQuery(Settings settings, Client client, QueryBuilder queryBuilder, String indice, String type) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setTypes(type)
			.setQuery(queryBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeFilter(Settings settings, Client client, FilterBuilder filterBuilder, String indice) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setPostFilter(filterBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeFilter(Settings settings, Client client, FilterBuilder filterBuilder, String indice, String type) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setTypes(type)
			.setPostFilter(filterBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeQueryHighlight(Settings settings, Client client, QueryBuilder queryBuilder, String indice, String field, String tag) throws Exception {
		SearchResponse searchResponse;
		
//		HighlightBuilder.Field hField = new HighlightBuilder.Field(field);
//		hField.highlightQuery(new TermQueryBuilder(field, "자전거"));
//		hField.preTags("<"+tag+">");
//		hField.postTags("</"+tag+">");
//		
		searchResponse = client.prepareSearch(indice)
			.setQuery(queryBuilder)
//			.addHighlightedField(hField)
			.addHighlightedField(field)
			.setHighlighterPreTags("<"+tag+">")
			.setHighlighterPostTags("</"+tag+">")
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeAggregation(Settings settings, Client client, QueryBuilder queryBuilder, AbstractAggregationBuilder aggsBuilder, String indice) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setQuery(queryBuilder)
			.addAggregation(aggsBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
	
	public static String executeAggregation(Settings settings, Client client, QueryBuilder queryBuilder, AbstractAggregationBuilder aggsBuilder, String indice, String type) throws Exception {
		SearchResponse searchResponse;
		
		searchResponse = client.prepareSearch(indice)
			.setTypes(type)
			.setQuery(queryBuilder)
			.addAggregation(aggsBuilder)
			.setTrackScores(true)
			.execute()
			.actionGet();
		
		return searchResponse.toString();
	}
}
