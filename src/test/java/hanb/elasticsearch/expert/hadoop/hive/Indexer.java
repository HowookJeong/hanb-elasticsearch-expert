package hanb.elasticsearch.expert.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// hive 를 이용해서 elasticsearch 로 등록 하기 위해서는 mapreducer 를 이용하기 때문에 실제 hadoop 이 설치된 위치에서 elasticsearch-hadoop-version.jar 가 함께 loading 되어서 실행이 되어야 한다.
// 그렇지 않을 경우 정상 동작 하지 않게 됨.
public class Indexer {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		String sql = "";
		Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
		Statement stmt = con.createStatement();
		ResultSet res = stmt.executeQuery("DROP TABLE artists");
				res = stmt.executeQuery("DROP TABLE artists_hive");
		res = stmt.executeQuery("create table artists_hive(seq int, name string, url string, picture string, ts string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'");
		res = stmt.executeQuery("LOAD DATA LOCAL INPATH '/Users/hwjeong/Documents/workspace/eclipse-j2ee/hanb-elasticsearch-expert/data/artists.dat' INTO TABLE artists_hive");

		res = stmt.executeQuery("CREATE EXTERNAL TABLE artists ("+
			"   id      BIGINT,"+
			"   seq     BIGINT,"+
			"   name     STRING,"+
			"   links   STRUCT<url:STRING, picture:STRING>,"+
			"   ts STRING) "+
			"STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' "+
			"TBLPROPERTIES('es.nodes' = 'localhost:9200'," +
			"	'es.port' = '9200'," +
			"	'es.resource' = 'radio/artists'," +
			"	'es._id.field' = 'id'," +
			"	'es.mapping.timestamp' = 'ts'," +
			"	'es.index.auto.create' = 'true')");
		
		
		sql = "INSERT OVERWRITE TABLE artists SELECT s.seq, s.seq, s.name, named_struct('url', s.url, 'picture', s.picture), s.ts FROM artists_hive s";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);

		res.close();
		stmt.close();
		con.close();
	}
}
