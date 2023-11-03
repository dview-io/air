package io.dview.air;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import static io.dview.air.Constants.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;


/**
 * This {@code AirQuerySubmitter} is responsible to submit Query in Pinot and result output in
 * Map<String, Object>
 */
@Slf4j
public class AirQuerySubmitter {
  private final AirConnectionPool airConnectionPool;

  public AirQuerySubmitter(AirConfiguration airConfiguration) throws SQLException {
    this.airConnectionPool = AirConnectionPool.create(airConfiguration.getEndPoint(),
        airConfiguration.getAuthToken(), airConfiguration.getPoolSize(), airConfiguration.isEnablePool());
  }

  /**
   * @param query linked with {@link String}
   * @return List linked with {@link List}
   * @throws SQLException linked with {@link Exception}
   */
  public List<Map<String, Object>> executeQuery(final String query) throws SQLException, JsonProcessingException {
    long startTime = System.currentTimeMillis();
    log.info("Query to be submitted: {}", query);
    Connection pinotConnection = this.airConnectionPool.getConnection();
    try (Statement statement = pinotConnection.createStatement()) {
      ResultSet rs = statement.executeQuery(query);
      int col = rs.getMetaData().getColumnCount();
      LinkedList<Map<String, Object>> queryResult = new LinkedList<>();
      while (rs.next()) {
        Map<String, Object> entry = Collections.synchronizedMap(new LinkedHashMap<>());
        for (int i = 1; i <= col; ++i) {
          if (rs.getMetaData().getColumnTypeName(i).contains(ARRAY))
            entry.put(rs.getMetaData().getColumnLabel(i), OBJECT_MAPPER.readValue(rs.getString(i), new TypeReference<>() {}));
          else if (rs.getMetaData().getColumnTypeName(i).contains(JSON))
            entry.put(rs.getMetaData().getColumnLabel(i), OBJECT_MAPPER.readValue(rs.getString(i), new TypeReference<>() {}));
          else entry.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
        }
        queryResult.add(entry);
      }
      log.debug("Total query time : {}, for query {}", (System.currentTimeMillis() - startTime), query);
      return queryResult;
    } finally {
      this.airConnectionPool.releaseConnection(pinotConnection);
    }
  }
}
