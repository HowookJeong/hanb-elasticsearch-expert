package hanb.elasticsearch.expert.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Searcher {
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
		ResultSet res; 
		res = stmt.executeQuery("DROP TABLE artists"); 
		res = stmt.executeQuery("CREATE EXTERNAL TABLE artists ("+
			"   id      BIGINT,"+
			"   seq     BIGINT,"+
			"   name     STRING,"+
			"   links   STRUCT<url:STRING, picture:STRING>) "+
			"STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' "+
			"TBLPROPERTIES('es.nodes' = 'localhost:9200', 'es.port' = '9200', 'es.resource' = 'radio/artists'," +
			" 'es.query' = '?q=name:b*')");
		
		
		sql = "select t2.* from artists t1 join artists_hive t2 on ( t2.seq = t1.id )";
		res = stmt.executeQuery(sql);
		
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t"
					+ res.getString(2) + "\t"
					+ res.getString(3) + "\t"
					+ res.getObject(4)
					);
		}
		
		res.close();
		stmt.close();
		con.close();
	}
}
