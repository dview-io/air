package io.dview.air;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * This {@code Constants} class is used to define
 * Constants that will be used across application.
 *
 */
public class Constants {
    private Constants(){ throw new IllegalStateException("Utils Can't Be Initialised");}
    public static final ObjectMapper OBJECT_MAPPER =new ObjectMapper(new YAMLFactory())
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    public static final String HEADER_NAME = "headers.Accept";
    public static final String HEADER_VALUE = "application/json";
    public static final String HEADER_AUTH_NAME = "headers.Authorization";

    // Query splitter
    public static final String QUERY_SPLITTER = ";";
}
