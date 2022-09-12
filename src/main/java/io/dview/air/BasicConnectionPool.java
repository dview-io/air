package io.dview.air;

import com.google.common.base.Strings;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.PinotDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * This {@code BasicConnectionPool} is responsible to maintain connection pooling and make sure each
 * call must take and return the connection.
 */
@Slf4j
public class BasicConnectionPool implements ConnectionPool {

  private final String endPoint;
  private final String authToken;
  private final boolean enablePool;
  private final LinkedBlockingDeque<Connection> connectionPool;
  private final LinkedBlockingDeque<Connection> usedConnections;

  public BasicConnectionPool(String endPoint, String authToken, List<Connection> pool, boolean enablePool) {
    this.endPoint = endPoint;
    this.authToken = authToken;
    this.usedConnections = new LinkedBlockingDeque<>();
    this.connectionPool = new LinkedBlockingDeque<>(pool);
    this.enablePool = enablePool;
  }

  public static BasicConnectionPool create(String endPoint, String authToken, int poolSize, boolean enablePool) throws SQLException {
    DriverManager.registerDriver(new PinotDriver());
    LinkedList<Connection> pool = new LinkedList<>();
    //Create connections pool on enabled
    if (enablePool)
      for (int i = 0; i < poolSize; i++) { pool.add(createConnection(endPoint, authToken)); }
    return new BasicConnectionPool(endPoint, authToken, pool, enablePool);
  }

  /**
   * @param endPoint linked with {@link String}
   * @param authToken linked with {@link String}
   * @return Connection linked with {@link Connection}
   * @throws SQLException linked with {@link Exception}
   */
  private static Connection createConnection(String endPoint, String authToken) throws SQLException {
    if (!Strings.isNullOrEmpty(authToken)) {
      Properties pinotProperties = new Properties();
      pinotProperties.put(Constants.HEADER_NAME, Constants.HEADER_VALUE);
      pinotProperties.put(Constants.HEADER_AUTH_NAME, authToken);
      log.info("The AirQuerySubmitter is being initialised");
      return DriverManager.getConnection(endPoint, pinotProperties);
    } else return DriverManager.getConnection(endPoint);
  }

  /** @return Connection linked with {@link Connection} */
  @Override
  public Connection getConnection() throws SQLException {
    //Get new connection on every execution if pool is disabled.
    if(!this.enablePool)
      return createConnection(this.endPoint, this.authToken);

    if (this.connectionPool.isEmpty() && usedConnections.isEmpty()) {
      connectionPool.add(createConnection(this.endPoint, this.authToken));
      log.info("New Connection On Empty Check, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
    } else if (this.connectionPool.isEmpty()) {
      connectionPool.add(createConnection(this.endPoint, this.authToken));
      log.info("New Connection Is Added, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
    }

    // Add a constraint check to have a valid connection check.
    Connection connection = this.connectionPool.removeFirst();
    if (connection.isClosed()) {
      connection = createConnection(this.endPoint, this.authToken);
      connectionPool.add(connection);
      log.info("New Connection On Closed Check, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
    }
    usedConnections.addLast(connection);
    return connection;
  }

  /**
   * @param connection linked with {@link Connection}
   * @return boolean linked with {@link Boolean}
   */
  @Override
  public boolean releaseConnection(Connection connection) throws SQLException {
    // Close the connection if pooling is off.
    if (!this.enablePool) {
      connection.close();
      return true;
    }

    if (connection.isClosed()) {
      connection = createConnection(this.endPoint, this.authToken);
      log.info("New Connection On Closed Check, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
    }
    this.connectionPool.addLast(connection);
    if (!usedConnections.isEmpty()) {
      usedConnections.removeFirst();
    }
    return true;
  }
}