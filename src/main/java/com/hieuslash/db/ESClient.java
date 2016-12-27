package com.hieuslash.db;
import com.hieuslash.WorkerBoot;
import com.hieuslash.WorkerConfig;
import java.util.Properties;
import java.util.Map;
import java.util.List;
import java.util.HashMap;

import org.elasticsearch.node.Node;
import org.elasticsearch.client.Client;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

@WorkerBoot
public class ESClient {
  private final Properties config;
  private final Node node;
  private final Client client;

  public ESClient() {
    config = WorkerConfig.getESConfig(ESClient.class);
    node = nodeBuilder().clusterName(config.getProperty("host")).node();
    client = node.client();
  }

  public void index(Map<String, Object> data) {
    client.prepareIndex("java_ads","ads",data.get("ad_id").toString()).setSource(data).execute().actionGet();
  }

  public void close() {
    node.close();
  }
}
