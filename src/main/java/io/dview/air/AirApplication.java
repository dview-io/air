package io.dview.air;

import java.io.File;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.IOException;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import static io.dview.air.Constants.OBJECT_MAPPER;
import static io.dview.air.Constants.QUERY_SPLITTER;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * This {@code AirApplication} is responsible to configure Pinot connection and gives direct
 * interface to write a query.
 */
@Slf4j
public class AirApplication {

  private final AirQuerySubmitter airQuerySubmitter;
  private final AirConfiguration airConfiguration;

  public AirApplication(AirConfiguration airConfiguration) throws SQLException {
    this.airConfiguration = airConfiguration;
    this.airQuerySubmitter = new AirQuerySubmitter(airConfiguration);
  }

  /**
   * @param args linked with {@link String}
   * @throws IOException linked with {@link Exception}
   * @throws SQLException linked with {@link Exception}
   */
  public static void main(String[] args) throws IOException, SQLException {
    AirConfiguration airConfiguration = OBJECT_MAPPER.findAndRegisterModules().readValue(new File(args[0]), AirConfiguration.class);
    log.info("The Application Configuration : {}", airConfiguration);
    AirApplication airApplication = new AirApplication(airConfiguration);
    long startTime = System.currentTimeMillis();
    Arrays.asList(airConfiguration.getQuery().split(QUERY_SPLITTER))
        .forEach(
            query -> {
              try {
                List<Map<String, Object>> results = airApplication.submitQuery(query);
                log.info("Query result : {}", results);
              } catch (SQLException | JsonProcessingException e) {
                log.error("Failed due to {}", Throwables.getStackTraceAsString(e.fillInStackTrace()));
              }
            });
    log.info("Final execution time for all queries in total : {}", (System.currentTimeMillis() - startTime));
  }

  /**
   * @param query linked with {@link String}
   * @return List linked with {@link List}
   * @throws SQLException linked with {@link Exception}
   */
  public List<Map<String, Object>> submitQuery(String query) throws SQLException, JsonProcessingException {
    return Strings.isNullOrEmpty(query) ? new ArrayList<>() : this.airQuerySubmitter.executeQuery(String.format("%s option(timeoutMs=%d)", query, this.airConfiguration.getTimeoutMs()));
  }

  /**
   * @param query linked with {@link String}
   * @return List linked with {@link List}
   * @throws SQLException linked with {@link Exception}
   */
  public List<Map<String, Object>> submitQuery(String query, long timeoutMs) throws SQLException, JsonProcessingException {
    return Strings.isNullOrEmpty(query) ? new ArrayList<>() : this.airQuerySubmitter.executeQuery(String.format("%s option(timeoutMs=%d)", query, timeoutMs));
  }
}