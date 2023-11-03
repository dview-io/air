package io.dview.air;

import java.sql.Connection;
import java.sql.SQLException;

public interface IConnectionPool {
    Connection getConnection() throws SQLException;
    boolean releaseConnection(Connection connection) throws SQLException;
}
