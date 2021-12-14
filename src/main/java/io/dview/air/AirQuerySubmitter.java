package io.dview.air;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

import static io.dview.air.Constants.OBJECT_MAPPER;

/**
 * This {@code AirQuerySubmitter} is responsible to submit Query in Pinot and result output in
 * Map<String, Object>
 */
@Slf4j
public class AirQuerySubmitter {
  private final BasicConnectionPool basicConnectionPool;

  public AirQuerySubmitter(AirConfiguration airConfiguration) throws SQLException {
    this.basicConnectionPool = BasicConnectionPool.create(airConfiguration.getEndPoint(),
            airConfiguration.getAuthToken(), airConfiguration.getPoolSize());
  }

  /**
   * @param query linked with {@link String}
   * @return List linked with {@link List}
   * @throws SQLException linked with {@link Exception}
   */
  public List<Map<String, Object>> executeQuery(final String query) throws SQLException, JsonProcessingException {
    Connection pinotConnection = this.basicConnectionPool.getConnection();
    try (Statement statement = pinotConnection.createStatement(); ) {
      ResultSet rs = statement.executeQuery(query);
      int col = rs.getMetaData().getColumnCount();
      LinkedList<Map<String, Object>> queryResult = new LinkedList<>();
      while (rs.next()) {
        Map<String, Object> entry = new HashMap<>();
        for (int i = 1; i <= col; ++i) {
          if (rs.getMetaData().getColumnTypeName(i).contains("ARRAY")) {
            List<Object> objects = OBJECT_MAPPER.readValue(rs.getString(i), new TypeReference<List<Object>>() {});
            entry.put(rs.getMetaData().getColumnLabel(i), objects);
          }else if (rs.getMetaData().getColumnTypeName(i).contains("JSON")){
            Map<String, Object> objects = OBJECT_MAPPER.readValue(rs.getString(i), new TypeReference<Map<String, Object>>() {});
            entry.put(rs.getMetaData().getColumnLabel(i), objects);
          }else entry.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
        }
        queryResult.add(entry);
      }
      return queryResult;
    } finally {
      this.basicConnectionPool.releaseConnection(pinotConnection);
    }
  }
}
