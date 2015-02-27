package hanb.elasticsearch.expert.hadoop.mr;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;

public class IndexSearcher {
	private static Random random = new Random();
	
	public static class SearchMapper extends Mapper {
		@Override
		public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
			Text docId = (Text) key;
			LinkedMapWritable doc = (LinkedMapWritable) value;
		
			System.out.println(docId);
			System.out.println(value);
			System.out.println(doc.keySet());
			System.out.println(doc.values());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.set(ConfigurationOptions.ES_NODES, "localhost:9200");
		conf.set(ConfigurationOptions.ES_RESOURCE, "radio/artists");
		HadoopCfgUtils.setGenericOptions(conf);
		
        Job job = new Job(conf);
        job.setInputFormatClass(EsInputFormat.class);
        job.setOutputKeyClass(Text.class);
        
        job.setOutputFormatClass(PrintStreamOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        boolean type = random.nextBoolean();
        Class<?> mapType = (type ? MapWritable.class : LinkedMapWritable.class);
        job.setOutputValueClass(mapType);
        
        conf.set(ConfigurationOptions.ES_QUERY, "{ \"query\" : { \"match_all\" : {} } }");
        job.setMapperClass(SearchMapper.class);
        job.setNumReduceTasks(0);
		
        boolean result = job.waitForCompletion(true);
	}
}
