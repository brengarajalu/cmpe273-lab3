package edu.sjsu.cmpe.cache.client;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");

        Collection<String> urlNodes= new ArrayList<String>();
        
        urlNodes.add("http://localhost:3000");
        urlNodes.add("http://localhost:3001");
        urlNodes.add("http://localhost:3002");
        
        HashMap<String,String> inpMap = new HashMap<String, String>();
        
        inpMap.put("1","a");
        inpMap.put("2","b");
        inpMap.put("3","c");
        inpMap.put("4","d");
        inpMap.put("5","e");
        inpMap.put("6","f");
        inpMap.put("7","g");
        inpMap.put("8","h");
		inpMap.put("9","i");
		inpMap.put("10","j");
		
		ConsistentHash<String> hash = new ConsistentHash<String>(3, urlNodes);
		
        System.out.println("Starting cache client");

        Iterator<Entry<String,String>> put = inpMap.entrySet().iterator();
        try{
        	System.out.println("Put");
        	while(put.hasNext())
        	{
        		Entry<String,String> entry = put.next();
        		String key = entry.getKey();
        		String nodeVal = hash.get(key);
        		CacheServiceInterface cacheService = new DistributedCacheService(nodeVal);
        		cacheService.put(Long.parseLong(key),entry.getValue());
        		System.out.println("put("+key+"=>"+ entry.getValue() +")");
        		//System.out.println(key+"-"+entry.getValue());
        	}
        	
        	 Iterator<Entry<String,String>> get = inpMap.entrySet().iterator();
        	 System.out.println("Get");
        	while(get.hasNext())
        	{
        		//Entry<String,String> entry = get.next();
        		String key = get.next().getKey();
        		String nodeVal = hash.get(key);
        		CacheServiceInterface cacheService = new DistributedCacheService(nodeVal);
        		String value= cacheService.get(Long.parseLong(key));
        		System.out.println("get("+key+"=>"+ value +")");
        		//System.out.println(key+"-"+value);
        	}
        	
        }
        catch (Exception e) {
			e.printStackTrace();
        }
        
        System.out.println("Existing Cache Client...");
      }
    
}
