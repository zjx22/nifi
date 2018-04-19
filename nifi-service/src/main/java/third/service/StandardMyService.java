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
package third.service;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({ "example"})
@CapabilityDescription("Example ControllerService implementation of MyService.")
public class StandardMyService extends AbstractControllerService implements MyService {
    //连接池增加url属性
    public static final PropertyDescriptor DatabaseConnectionURL = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("Database Connection URL")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();



    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DatabaseConnectionURL);

        properties = Collections.unmodifiableList(props);
    }

    private Map<String, String> map;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        map = new HashMap<>();

        String Urlvalue = context.getProperty(DatabaseConnectionURL).getValue();

        map.put("Database_URL", Urlvalue);



    }

    @OnDisabled
    public void shutdown() {
        map = null;
    }

    @Override
    public Map<String, String> getMap() {
        return map;
    }

}
