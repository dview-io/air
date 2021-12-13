package io.dview.air;

import lombok.extern.slf4j.Slf4j;
import org.apache.pinot.client.PinotDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

/**
 * This {@code BasicConnectionPool} is responsible to maintain
 * connection pooling and make sure each call must take and
 * return the connection.
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
        for (int i = 0; i < poolSize; i++) {
            pool.add(createConnection(endPoint, authToken));
        }
        return new BasicConnectionPool(endPoint, authToken, pool);
    }

    /**
     * @param endPoint  linked with {@link String}
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
        } else
            return DriverManager.getConnection(endPoint);
    }

    /**
     * @return Connection linked with {@link Connection}
     */
    @Override
    public synchronized Connection getConnection() throws SQLException {
        if (this.connectionPool.isEmpty() && usedConnections.isEmpty()) {
            connectionPool.add(createConnection(this.endPoint, this.authToken));
        } else if (this.connectionPool.isEmpty()) {
            while (this.connectionPool.size() - 1 < 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        Connection connection = this.connectionPool.removeFirst();
        usedConnections.addLast(connection);
        return connection;
    }

    /**
     * @param connection linked with {@link Connection}
     * @return boolean linked with {@link Boolean}
     */
    @Override
    public synchronized boolean releaseConnection(Connection connection) {
        this.connectionPool.addLast(connection);
        return Objects.nonNull(usedConnections.removeFirst());
    }
}