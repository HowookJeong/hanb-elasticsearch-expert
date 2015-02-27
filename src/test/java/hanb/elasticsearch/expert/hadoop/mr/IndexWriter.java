package hanb.elasticsearch.expert.hadoop.mr;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.elasticsearch.hadoop.mr.HadoopCfgUtils;
import org.elasticsearch.hadoop.mr.LinkedMapWritable;
import org.elasticsearch.hadoop.util.WritableUtils;

public class IndexWriter {
	public static class TabMapper extends Mapper {
        @Override
        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            Map<String, String> entry = new LinkedHashMap<String, String>();

            entry.put("number", st.nextToken());
            entry.put("name", st.nextToken());
            entry.put("url", st.nextToken());

            if (st.hasMoreTokens()) {
                String str = st.nextToken();
                if (str.startsWith("http")) {
                    entry.put("picture", str);

                    if (st.hasMoreTokens()) {
                        String token = st.nextToken();
                        entry.put("@timestamp", token);
                    }
                }
                else {
                    entry.put("@timestamp", str);
                }
            }
            
            context.write(key, WritableUtils.toWritable(entry));
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
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapOutputValueClass(LinkedMapWritable.class);
		job.setMapperClass(TabMapper.class);
		job.setNumReduceTasks(0);
        
        // job split
		File fl = new File("data/artists.dat");
		long splitSize = fl.length() / 3;
		TextInputFormat.setMaxInputSplitSize(job, splitSize);
		TextInputFormat.setMinInputSplitSize(job, 50);
		
		// text
//		Job standard = new Job(job.getConfiguration());
//		standard.setMapperClass(TabMapper.class);
//        standard.setMapOutputValueClass(LinkedMapWritable.class);
//        TextInputFormat.addInputPath(standard, new Path("data/artists.dat"));
//        
//        boolean result = standard.waitForCompletion(true);

		// json
        Job json = new Job(job.getConfiguration());
		TextInputFormat.addInputPath(json, new Path("data/artists.json"));
		json.setMapperClass(Mapper.class);
		json.setMapOutputValueClass(Text.class);
		json.getConfiguration().set(ConfigurationOptions.ES_INPUT_JSON, "true");

        boolean result = json.waitForCompletion(true);
	}
}
