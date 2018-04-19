package third.processors.demo;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.sql.*;
import java.util.*;

public class JDBCtest {


    Map<String,MysqlData> mysqlMap=new HashMap<>();
        Jedis jedis =null;


    public  Map outMap(String URL,String sql) {
        String driver = "com.mysql.jdbc.Driver";
        //String URL = "jdbc:mysql://10.1.2.49:3306/test";
        Connection con = null;
        ResultSet rs = null;
        Statement st = null;
        // String sql = "select * from regulation";

        try
        {
            Class.forName(driver);
        }
        catch(java.lang.ClassNotFoundException e)
        {
            // System.out.println("Connect Successfull.");
            System.out.println("Cant't load Driver");
        }
        try
        {
            con=DriverManager.getConnection(URL,"root","123456");
            st=con.createStatement();
            rs=st.executeQuery(sql);

            while(rs.next()) {
                MysqlData mysqldata=new MysqlData();
                mysqldata.setName(rs.getString(2));
                mysqldata.setLower(rs.getDouble(3));
                mysqldata.setUpper(rs.getDouble(4));
                mysqldata.setLevel(rs.getInt(5));
                mysqlMap.put(mysqldata.name,mysqldata);

            }
            rs.close();
            st.close();
            con.close();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }



        //输出map内容
       /* Iterator iterator=mysqlMap.keySet().iterator();
        while (iterator.hasNext()){
            String name2=iterator.next().toString();
            System.out.println(name2);
        }*/

        return  mysqlMap;

    }

    public String computeAndStoreJson(String source,Map<String,MysqlData> myMap,Jedis jedis){
        int status=0;
        int level=0;
        JSONObject jsonObject=new JSONObject(source);
        String id=jsonObject.getString("user_id");
        JSONArray dataArray=jsonObject.getJSONArray("data");
        JSONArray afterARR=new JSONArray();
        JSONObject lastjsonObj=new JSONObject();
        //循环遍历json数组，
        for (int i = 0; i < dataArray.length(); i++) {
            JSONObject jsonObject2 = dataArray.getJSONObject(i);
            if (jsonObject2 != null) {
                Iterator iterator = jsonObject2.keySet().iterator();
                while (iterator.hasNext()) {
                    String keyname = iterator.next().toString();

                    //判断对应的key
                    if (myMap.containsKey(keyname)) {
                        //首先获取json数组中k对应的值
                        Double keyvalue = jsonObject2.getDouble(keyname);
                        //判断是否正常
                        MysqlData data = myMap.get(keyname);
                        if (data.getLevel() == 1) {//重要指标
                            level=1;
                            if (keyvalue > data.getUpper()) {
                                status = 1;
                            } else if (keyvalue < data.getLower() ){
                                status = -1;
                            }else {
                                status=0;
                            }

                        } else {
                            level=2;
                            if (keyvalue > data.getUpper()) {
                                status = 1;
                            } else if (keyvalue < data.getLower()) {
                                status = -1;
                            }else {
                                status=0;
                            }

                        }
                    //进行存储，
                        Map<String,Double> mapstore=new HashMap<>();
                        mapstore.put(keyname,keyvalue);
                        mapstore.put("status",(double)status);
                        mapstore.put("level",(double)level);
                        //存储进redis
                        JSONObject obj=new JSONObject(mapstore);

                        jedis.hset(id+"_zjx",keyname,obj.toString());

                    }
                }
            }

        }
        return  id;
    }

    public Jedis conTOJedis(String url){


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

        sentinels.add(url);

        System.out.println(sentinels);

        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool("masterTest", sentinels, config, "Kingleading");

        jedis = jedisSentinelPool.getResource();

        return  jedis;
    }

    public  JSONObject getAll(String key,Jedis jedis){
        int imporSum=0;
        int otherSum=0;
        double status=0;
        int star=0;
        double level=0;
        //a.获取所有id，返回类型为map
        //map展示<"a","{"a":1,"status":2,"level": 1}">
        Map<String,String> test=jedis.hgetAll(key+"_zjx");

            JSONArray jsarr = new JSONArray();
            Iterator iterator1= test.values().iterator();
            while (iterator1.hasNext()) {
                String arr = iterator1.next().toString();
                JSONObject js = new JSONObject(arr);
                //取出js里的status和level值
                status = js.getDouble("status");
                level = js.getDouble("level");
                if (level == 1) {
                    if (status != 0) {
                        imporSum++;
                    }
                } else {
                    if (status != 0) {
                        otherSum++;
                    }
                }
                js.remove("level");
                jsarr.put(js);
            }
        //判断星级
        if (imporSum == 0 && otherSum == 0) {
            star = 5;
        } else if (imporSum == 0 && otherSum <= 3) {
            star = 4;
        } else if (imporSum == 0 && otherSum > 3) {
            star = 3;
        } else if (imporSum <= 3 && otherSum <= 3) {
            star = 2;
        } else if (imporSum >= 3 && otherSum > 3) {
            star = 1;
        }


        //拼接成指定输出格式
        jedis.close();
        JSONObject jsonObject1=new JSONObject();
        jsonObject1.put("user_id",key);
        jsonObject1.put("data",jsarr);
        jsonObject1.put("star",star);

        return jsonObject1;

    }




    public static void main(String[] args){
            JDBCtest test=new JDBCtest();
        String url="10.1.24.215:17003";
        //new JDBCtest().test( new JDBCtest().outMap(),j);

        String source = "{'user_id':'fasfdsf6s876fs7d6f7ds6f','data':[{'a':32},{'b':55.2},{'d':1},{'c':34}]}";
        //1.取mysql数据标准
        String URL = "jdbc:mysql://10.1.2.49:3306/test";
        String sql = "select * from regulation";
        test.mysqlMap=test.outMap(URL,sql);
        Jedis jedis=test.conTOJedis(url);
        //2.进行计算，存储
        String id=test.computeAndStoreJson(source,test.mysqlMap,jedis);
        JSONObject jsonObject= test.getAll(id,jedis);
        System.out.println(jsonObject);


    }

}
