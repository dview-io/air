package io.dview.air;

import com.google.common.base.Throwables;
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
  private final LinkedList<Connection> connectionPool;
  private final LinkedList<Connection> usedConnections = new LinkedList<>();

  public BasicConnectionPool(String endPoint, String authToken, LinkedList<Connection> pool) {
    this.endPoint = endPoint;
    this.authToken = authToken;
    this.connectionPool = pool;
  }

  public static BasicConnectionPool create(String endPoint, String authToken, int poolSize) throws SQLException {
    DriverManager.registerDriver(new PinotDriver());
    LinkedList<Connection> pool = new LinkedList<>();
    for (int i = 0; i < poolSize; i++) { pool.add(createConnection(endPoint, authToken)); }
    return new BasicConnectionPool(endPoint, authToken, pool);
  }

  /**
   * @param endPoint linked with {@link String}
   * @param authToken linked with {@link String}
   * @return Connection linked with {@link Connection}
   * @throws SQLException linked with {@link Exception}
   */
  private static Connection createConnection(String endPoint, String authToken) throws SQLException {
    if (authToken != null) {
      Properties pinotProperties = new Properties();
      pinotProperties.put(Constants.HEADER_NAME, Constants.HEADER_VALUE);
      pinotProperties.put(Constants.HEADER_AUTH_NAME, authToken);
      log.info("The AirQuerySubmitter is being initialised");
      return DriverManager.getConnection(endPoint, pinotProperties);
    } else return DriverManager.getConnection(endPoint);
  }

  /** @return Connection linked with {@link Connection} */
  @Override
  public synchronized Connection getConnection() throws SQLException {
    if (this.connectionPool.isEmpty() && usedConnections.isEmpty()) {
      connectionPool.add(createConnection(this.endPoint, this.authToken));
      log.info("New Connection On Empty Check, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
    } else if (this.connectionPool.isEmpty()) {
      while (this.connectionPool.size() - 1 < 0) {
        try {
          wait();
        } catch (InterruptedException ex) {
          log.error("Failed due to : {}", Throwables.getStackTraceAsString(ex.fillInStackTrace()));
          connectionPool.add(createConnection(this.endPoint, this.authToken));
          log.info("New Connection Is Added, Size {}, {}", this.connectionPool.size(), this.usedConnections.size());
        }
      }
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
  public synchronized boolean releaseConnection(Connection connection) throws SQLException {
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
