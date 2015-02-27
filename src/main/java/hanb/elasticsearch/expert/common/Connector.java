package hanb.elasticsearch.expert.common;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class Connector {
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * Settings 생성 시 cluster 명을 등록 한다.<br>
     * 
     * @param clusterName		cluster 명.
     * @throws Exception
     */
	public static Settings buildSettings(String clusterName) {
		Settings settings;
		
		settings = ImmutableSettings.settingsBuilder()
									.put("cluster.name", clusterName)
									.put("client.transport.sniff", true)
									.put("network.tcp.blocking", false)
									.put("client.transport.ping_timeout", "10s")
									.build();

		return settings;
	}
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * elasticsearch client TransportAddress 등록.<br>
     * 	cluster.node.list<br>
     * 
     * @param settings		client settings 정보.
     * @throws Exception
     */
	public static Client buildClient(Settings settings, String[] nodes) throws Exception {
		TransportClient client = new TransportClient(settings);
		int nodeSize = nodes.length;

		for (int i = 0; i < nodeSize; i++) {
			client.addTransportAddress(toAddress(nodes[i]));
		}

		return client;
	}
	
	/**
     * Reference : https://github.com/dadoonet/spring-elasticsearch
     * 
     * InetSocketTransportAddress 등록.<br>
     * 
     * @param address		node 들의 ip:port 정보.
     * @return InetSocketTransportAddress
     * @throws Exception
     */
	public static InetSocketTransportAddress toAddress(String address) {
		if (address == null) return null;
		
		String[] splitted = address.split(":");
		int port = 9300;
		
		if (splitted.length > 1) {
			port = Integer.parseInt(splitted[1]);
		}
		
		return new InetSocketTransportAddress(splitted[0], port);
	}
}
