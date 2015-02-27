package hanb.elasticsearch.expert.func;

import hanb.elasticsearch.expert.common.Connector;
import hanb.elasticsearch.expert.common.Operators;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutoCompletionTest {
	private static final Logger log = LoggerFactory.getLogger(AutoCompletionTest.class);
	
	@Ignore
	public void testPrefixQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		PrefixQueryBuilder queryBuilder = new PrefixQueryBuilder("keyword_prefix", "elastic");
		String searchResult = Operators.executeQuery(settings, client, queryBuilder, "autocompletion");
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testPrefixQueryHighlight() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		PrefixQueryBuilder queryBuilder = new PrefixQueryBuilder("keyword_prefix", "elastic");
		String searchResult = Operators.executeQueryHighlight(settings, client, queryBuilder, "autocompletion", "keyword_prefix", "strong");
		
		log.debug(searchResult);
		// not_analyzed field 이기 때문에 전체 값에 대한 강조 처리가 된다.
		// 입력한 값에 대한 강조 처리를 위해서는 ngram 또는 edgeNgram 을 적용해서 term vector 정보를 이용한 강조 처리를 해줘야 한다.
	}
	
	@Ignore
	public void testNgramQuery() throws Exception {
		// http://localhost:9200/autocompletion/_analyze?analyzer=ngram_analyzer&text=ucene
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword", "ucene");
		String searchResult = Operators.executeQuery(settings, client, queryBuilder, "autocompletion");
		
		log.debug(searchResult);
		
		// ngram 의 경우 앞의 첫 문자 부터 시작해서 max length 만큼 token 을 만들어 낸다.
		// tokenizer 정의 시 trim 을 적용했기 때문에 whitespace 에 대한 처리가 올바르지 않다. (whitespace를 제거 하거나 trim 을 제거 해야 함.)
	}
	
	@Ignore
	public void testNgramQueryHighlight() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword", "ucene");
		String searchResult = Operators.executeQueryHighlight(settings, client, queryBuilder, "autocompletion", "keyword", "strong");
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testEdgeNgramQuery() throws Exception {
		// http://localhost:9200/autocompletion/_analyze?analyzer=edge_ngram_analyzer&text=lucen
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword_edge", "lucen");
		String searchResult = Operators.executeQuery(settings, client, queryBuilder, "autocompletion");
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testEdgeNgramQueryHighlight() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword_edge", "lucen");
		String searchResult = Operators.executeQueryHighlight(settings, client, queryBuilder, "autocompletion", "keyword_edge", "strong");
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testEdgeNgramBackQuery() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword_edge_back", "e");
		String searchResult = Operators.executeQuery(settings, client, queryBuilder, "autocompletion");
		
		log.debug(searchResult);
	}
	
	@Ignore
	public void testEdgeNgramBackQueryHighlight() throws Exception {
		Settings settings = Connector.buildSettings("elasticsearch");
		Client client = Connector.buildClient(settings, new String[] {"localhost:9300"});
		
		TermQueryBuilder queryBuilder = new TermQueryBuilder("keyword_edge_back", "e");
		String searchResult = Operators.executeQueryHighlight(settings, client, queryBuilder, "autocompletion", "keyword_edge_back", "strong");
		
		log.debug(searchResult);
	}
}
