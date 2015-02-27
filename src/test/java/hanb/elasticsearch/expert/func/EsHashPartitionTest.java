package hanb.elasticsearch.expert.func;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.routing.operation.hash.HashFunction;
import org.elasticsearch.cluster.routing.operation.hash.djb.DjbHashFunction;
import org.elasticsearch.common.math.MathUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsHashPartitionTest {
	private static final Logger log = LoggerFactory.getLogger(EsHashPartitionTest.class);
	private HashFunction hashFunction = new DjbHashFunction();
	
	@Test
	public void testHashPartition() {
		int shardSize = 120;
		List<Long> shards = new ArrayList<Long>();
		long[] partSize = new long[shardSize];
		
		for ( int i=0; i<shardSize; i++ ) {
			shards.add((long) 0);
			partSize[i] = 0;
		}
		
		for ( int i=0; i<100000; i++ ) {
			int shardId = MathUtils.mod(hash(String.valueOf(i)), shardSize);
			shards.add(shardId, (long) ++partSize[shardId]);
		}
		
		for ( int i=0; i<shardSize; i++ ) {
			log.debug("["+i+"] {}", partSize[i]);
		}
	}
	
	public int hash(String routing) {
		return hashFunction.hash(routing);
	}
}

