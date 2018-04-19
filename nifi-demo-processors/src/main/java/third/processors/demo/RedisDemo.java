package third.processors.demo;

import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.*;

/**
 * Created by NightWatch on 2018/4/13.
 */
public class RedisDemo {
    public static void main(String[] args) {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(5000);
        config.setMaxIdle(256);
        config.setMaxWaitMillis(5000L);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setTestWhileIdle(true);
        config.setMinEvictableIdleTimeMillis(60000L);
        config.setTimeBetweenEvictionRunsMillis(3000L);
        config.setNumTestsPerEvictionRun(-1);

        Set<String> sentinels = new HashSet<>();

        sentinels.add("10.1.24.215:17003");

        System.out.println(sentinels);

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("masterTest", sentinels, config, "Kingleading");

        Jedis jedis = jedisSentinelPool.getResource();


        String source="{'user_id':'fasfdsf6s876fs7d6f7ds6f','data':[{'a':0},{'b':85.2},{'c':22}]}";
        JSONObject jsonObject=new JSONObject(source);
        Map<String,Double> mm=new HashMap<>();
        mm.put("a",1.0);
       mm.put("status",-1.0);
        JSONObject obj=new JSONObject(mm);
        Map<String,Double> mm2=new HashMap<>();
        mm2.put("b",3.0);
        mm2.put("status",-2.0);
        JSONObject obj2=new JSONObject(mm2);


      jedis.hset("1","a",obj.toString());
        jedis.hset("1","b",obj2.toString());

       Map<String,String> map=jedis.hgetAll("1");


        Iterator iterator=map.keySet().iterator();
        while (iterator.hasNext()){
                String key=iterator.next().toString();
                String value=map.get(key);
                System.out.println(key+"   "+value);
        }
        JSONArray jsarr = new JSONArray();
        Iterator iterator1= map.values().iterator();
        while (iterator1.hasNext()) {
            String arr = iterator1.next().toString();
            JSONObject js = new JSONObject(arr);
           // System.out.println(js.get("a"));
            System.out.println(js.keySet().iterator().next());
            System.out.println(js.get(js.keySet().iterator().next()));


            js.remove("status");
           // System.out.println(js);

            jsarr.put(js);
        }

        System.out.println(jsarr);
        jedis.del("1");

        jedis.close();
        int a=1;
        //double b=a;
      //  System.out.println(((double) a));



    }

}
