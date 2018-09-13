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

package org.apache.nifi.processors.ccda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Collections;
import java.util.stream.Collectors;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"CCDA", "healthcare", "OMOP", "json", "attributes", "flowfile"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Generates a JSON representation of the input FlowFile CCDA Attributes. The resulting JSON " +
        "will be written to either JSON Attributes or the FlowFile as content. Either way, they will be formatted " +
        "to easily be parsed for OMOP database schema.")
@WritesAttribute(attribute = "OmopJSONAttributes", description = "JSON representation of Attributes for OMOP")
public class CCDAAttributesToOmop extends AbstractProcessor {

    public static final String JSON_ATTRIBUTE_NAME = "OmopStageJsonAttributes";
    private static final String AT_LIST_SEPARATOR = ",";

    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String APPLICATION_JSON = "application/json";

    public static final PropertyDescriptor POSTGRESQL_URL = new PropertyDescriptor.Builder()
            .name("PostgreSQL URL")
            .description("URL for JDBC to access postgresql database. Must have write permission granted. "
            		+ "Parsed data will not be written to database if this field is empty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor POSTGRESQL_CREDENTIAL = new PropertyDescriptor.Builder()
            .name("PostgreSQL Credential")
            .description("Colon separated username and password. PostgreSQL URL must be set.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
            .name("Destination")
            .description("Control if JSON value is written as a new flowfile attribute '" + JSON_ATTRIBUTE_NAME + "' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_ATTRIBUTE)
            .build();

//    public static final PropertyDescriptor INCLUDE_CORE_ATTRIBUTES = new PropertyDescriptor.Builder()
//            .name("Include Core Attributes")
//            .description("Determines if the FlowFile org.apache.nifi.flowfile.attributes.CoreAttributes which are " +
//                    "contained in every FlowFile should be included in the final JSON value generated.")
//            .required(true)
//            .allowableValues("true", "false")
//            .defaultValue("true")
//            .build();
//
    public static final PropertyDescriptor NULL_VALUE_FOR_EMPTY_STRING = new PropertyDescriptor.Builder()
            .name(("Null Value"))
            .description("If true a non existing or empty attribute will be NULL in the resulting JSON. If false an empty " +
                    "string will be placed in the JSON")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully converted attributes to JSON").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to convert attributes to JSON").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    private JsonNode jsonNode;

    
    private static final ObjectMapper objectMapper = new ObjectMapper();
//    private volatile Set<String> attributesToRemove;
    private volatile Set<String> attributes;
    private volatile Boolean nullValueForEmptyString;
    private volatile boolean destinationContent;
    private volatile Pattern pattern;

    private volatile String postgresqlUrl = null;
    private volatile String postgresqlUsername = null;
    private volatile String postgresqlPassword = null;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(POSTGRESQL_URL);
        properties.add(POSTGRESQL_URL);
        properties.add(DESTINATION);
//        properties.add(INCLUDE_CORE_ATTRIBUTES);
        properties.add(NULL_VALUE_FOR_EMPTY_STRING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Builds the JSON String from attributes (includes mapping to Omop table).
     * 
     * @return
     *   JSON String that are feed to a Jackson ObjectMapper
     */
    protected String buildAttributesJSONForFlowFile(FlowFile ff, Set<String> attributes) {
    	if (attributes != null) {
    		return null;
    	} else {
    		return null;
    	}
    }

    /**
     * Builds the Map of attributes that should be included in the JSON that is emitted from this process.
     *
     * @return
     *  Map of values that are feed to a Jackson ObjectMapper
     */
    protected Map<String, String> buildAttributesMapForFlowFile(FlowFile ff, Set<String> attributes, Set<String> attributesToRemove,
            boolean nullValForEmptyString, Pattern attPattern) {
        Map<String, String> result;
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (attributes != null || attPattern != null) {
            result = new HashMap<>();
            if(attributes != null) {
                for (String attribute : attributes) {
                    String val = ff.getAttribute(attribute);
                    if (val != null || nullValForEmptyString) {
                        result.put(attribute, val);
                    } else {
                        result.put(attribute, "");
                    }
                }
            }
            if(attPattern != null) {
                for (Map.Entry<String, String> e : ff.getAttributes().entrySet()) {
                    if(attPattern.matcher(e.getKey()).matches()) {
                        result.put(e.getKey(), e.getValue());
                    }
                }
            }
        } else {
            Map<String, String> ffAttributes = ff.getAttributes();
            result = new HashMap<>(ffAttributes.size());
            for (Map.Entry<String, String> e : ffAttributes.entrySet()) {
                if (!attributesToRemove.contains(e.getKey())) {
                    result.put(e.getKey(), e.getValue());
                }
            }
        }
        return result;
    }

    private Set<String> buildAtrs(String atrList, Set<String> atrsToExclude) {
        //If list of attributes specified get only those attributes. Otherwise write them all
        if (StringUtils.isNotBlank(atrList)) {
            String[] ats = StringUtils.split(atrList, AT_LIST_SEPARATOR);
            if (ats != null) {
                Set<String> result = new HashSet<>(ats.length);
                for (String str : ats) {
                    String trim = str.trim();
                    if (!atrsToExclude.contains(trim)) {
                        result.add(trim);
                    }
                }
                return result;
            }
        }
        return null;
    }

    private void setPostgresInfo (String url, String credential) {
    	if (url != null && !url.isEmpty()) {
    		postgresqlUrl = url.trim();
    	}
    	
    	if (this.postgresqlUrl != null && credential != null && !credential.isEmpty()) {
    		String[] credentialItems = credential.split(":");
    		if (credentialItems.length == 2) {
    			postgresqlUsername = credentialItems[0].trim();
    			postgresqlPassword = credentialItems[1].trim();
    		} else {
    			// We have invalid credential information. Set URL to null as well.
    			postgresqlUrl = null;
    		}
    	}
    }
    
    private void loadJsonTemplate() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        try (InputStream is = classloader.getResourceAsStream("template.json")){
        	jsonNode = objectMapper.readTree(is);
            // each child element is key#value and multiple elements are separated by @
            for (String property : mappings.stringPropertyNames()) {
                String[] variables = StringUtils.split(mappings.getProperty(property), FIELD_SEPARATOR);
                Map<String, String> map = new LinkedHashMap<String, String>();
                for (String variable : variables) {
                    String[] keyvalue = StringUtils.split(variable, KEY_VALUE_SEPARATOR);
                    map.put(keyvalue[0], keyvalue[1]);
                }
                processMap.put(property, map);
            }

        } catch (IOException e) {
            getLogger().error("Failed to load mappings", e);
            throw new ProcessException("Failed to load mappings", e);
        }    	
    }
    
    @OnScheduled
    public void onScheduled(ProcessContext context) {
    	// Get PostgreSQL information if exists.
    	setPostgresInfo(context.getProperty(POSTGRESQL_URL).getValue(), context.getProperty(POSTGRESQL_CREDENTIAL).getValue());
    	
    	// Read JSON template file and get Java Object ready for JSON container.
    	loadJsonTemplate();
    	
//        attributesToRemove = context.getProperty(INCLUDE_CORE_ATTRIBUTES).asBoolean() ? Collections.EMPTY_SET : Arrays.stream(CoreAttributes.values())
//                .map(CoreAttributes::key)
//                .collect(Collectors.toSet());
//        attributes = buildAtrs(context.getProperty(ATTRIBUTES_MAP).getValue(), attributesToRemove);
        nullValueForEmptyString = context.getProperty(NULL_VALUE_FOR_EMPTY_STRING).asBoolean();
        destinationContent = DESTINATION_CONTENT.equals(context.getProperty(DESTINATION).getValue());
//        if(context.getProperty(ATTRIBUTES_REGEX).isSet()) {
//            pattern = Pattern.compile(context.getProperty(ATTRIBUTES_REGEX).evaluateAttributeExpressions().getValue());
//        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final Map<String, String> atrList = buildAttributesMapForFlowFile(original, attributes, attributesToRemove, nullValueForEmptyString, pattern);

        try {
            if (destinationContent) {
                FlowFile conFlowfile = session.write(original, (in, out) -> {
                    try (OutputStream outputStream = new BufferedOutputStream(out)) {
                        outputStream.write(objectMapper.writeValueAsBytes(atrList));
                    }
                });
                conFlowfile = session.putAttribute(conFlowfile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
                session.transfer(conFlowfile, REL_SUCCESS);
            } else {
                FlowFile atFlowfile = session.putAttribute(original, JSON_ATTRIBUTE_NAME, objectMapper.writeValueAsString(atrList));
                session.transfer(atFlowfile, REL_SUCCESS);
            }
        } catch (JsonProcessingException e) {
            getLogger().error(e.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
