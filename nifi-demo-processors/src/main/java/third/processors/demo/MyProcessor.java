/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package third.processors.demo;



import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import third.service.MyService;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor sqlText = new PropertyDescriptor.Builder()
            .name("sql语句").description("safdsf")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor RedisURL = new PropertyDescriptor.Builder()
            .name("redis连接url").description("safdsf")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();


    public static final PropertyDescriptor map = new PropertyDescriptor.Builder()
            .name("JDBC连接设置")
            .identifiesControllerService(MyService.class).required(true).build();


    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("MY_RELATIONSHIP")
            .description("Example relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("fasdf").build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

//sql jdbc
    String driver = "com.mysql.jdbc.Driver";
    String URL = null;
    String redisUrl=null;
    Connection con = null;
    ResultSet rs = null;
    Statement st = null;
    String sql = null;
    Jedis jedis =null;
//存放标准数据的map
    Map<String,MysqlData> mysqlMap=new HashMap<>();



    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(sqlText);
        descriptors.add(map);
        descriptors.add(RedisURL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
}

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        MyService myService = context.getProperty(map).asControllerService(MyService.class);

        Map<String, String> valueMap = myService.getMap();
        URL=valueMap.get("Database_URL");
        sql = context.getProperty(sqlText).getValue();
        redisUrl=context.getProperty(redisUrl).getValue();
        //1.调用方法，将健康标准存入map
        JDBCtest t=new JDBCtest();
       mysqlMap=t.outMap(URL,sql);
        jedis=t.conTOJedis(redisUrl);
}




    @OnUnscheduled
    public void unscheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if ( flowFile == null ) {
            return;
        }
        // TODO implement

        try {


            final byte[] buffer = new byte[(int)flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                       int bytesRead = 0;
                    int len;
                    while (bytesRead < buffer.length) {
                    len = in.read(buffer, bytesRead, buffer.length - bytesRead);
                    if (len < 0) {
                        throw new EOFException();
                    }
                    bytesRead += len;
                }



                }
            });

           JDBCtest test=new JDBCtest();
           // String source = "{'user_id':'fasfdsf6s876fs7d6f7ds6f','data':[{'a':75},{'b':85.2},{'d':85.2},{'c':22}]}";
            //1.取mysql数据标准,在onschedule里初始化

           String source=new String(buffer);

            //2.进行计算，存储
            String id=test.computeAndStoreJson(source,mysqlMap,jedis);
            JSONObject jsonObject= test.getAll(id,jedis);

            //String content = new String(buffer, StandardCharsets.UTF_8);

            FlowFile write = session.write(flowFile, out -> {
                out.write((jsonObject.toString()).getBytes());
        });
            session.transfer(write, MY_RELATIONSHIP);

        } catch (Exception e) {

            getLogger().error("MyProcessor", e);
            session.transfer(flowFile, FAILURE);


        }


    }



}
