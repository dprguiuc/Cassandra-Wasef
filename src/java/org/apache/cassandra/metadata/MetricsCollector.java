package org.apache.cassandra.metadata;

import java.util.ArrayList;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;

import java.lang.reflect.*;

public class MetricsCollector {
	
	private String[] target;
	private String[] metrics;
	
	public MetricsCollector(String target, String metrics){
		this.metrics = metrics.split(";");
		this.target = target.split("\\.");
	}

	public String getMetrics() {
		String collectedMetrics = "";
		try{
			for(String metric: metrics){
				
				String[] specs = metric.split(",");
				String type = specs[0].trim().substring(specs[0].trim().indexOf("type=")+5);
				String scope = specs[1].trim().substring(specs[0].trim().indexOf("scope=")+7);
				String name = specs[2].trim().substring(specs[0].trim().indexOf("name=")+6);
				
				if(type.equals("ColumnFamily") || metric.equals("IndexColumnFamily")){
					String keyspace = target[0];
					String columnFamily = "user"; //target[1];
					collectedMetrics += getColumnFamilyMetrics(keyspace, columnFamily, name);
				}
			}
		}catch(Exception ex){
			collectedMetrics = "";
		}
		return collectedMetrics;
	}

	
	
	private String getColumnFamilyMetrics(String keyspace, String columnFamily, String metricName){
		
		String collectedMetrics;
		try{
			ColumnFamilyStore cfs = Table.open(keyspace).getColumnFamilyStore(columnFamily);
			
			Method method = cfs.getClass().getMethod("get" + metricName, null);
			
			collectedMetrics =  metricName + ":" + method.invoke(cfs, null).toString();
		}catch(Exception ex){
			collectedMetrics = "";
		}
		return collectedMetrics;
	}


}
