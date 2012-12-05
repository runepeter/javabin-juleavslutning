package org.brylex.javabin.db;

import com.jolbox.bonecp.BoneCPDataSource;
import org.brylex.javabin.config.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.TimeUnit;

public class Fluffy {

    private static final Logger logger = LoggerFactory.getLogger(Fluffy.class);

    private static class JallaDataSource implements DataSource {

        private final DataSource delegate;

        private int count = 0;

        private JallaDataSource(final DataSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public Connection getConnection() throws SQLException {

            /*if (++count > 1 && count < 5) {
                throw new SQLException("Her simulerer vi en julefeil");
            }*/

            return new FluffyConnection(delegate.getConnection());
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return delegate.getConnection(username, password);
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

    public static void main(String[] args) {

        BoneCPDataSource d = new BoneCPDataSource();
        d.setDatasourceBean(new JallaDataSource(Database.newDataSource()));
        d.setAcquireIncrement(1);
        d.setAcquireRetryAttempts(2);
        d.setAcquireRetryDelay(3, TimeUnit.SECONDS);

        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;

        try {

            connection = d.getConnection();
            logger.info("connection :: [{}]", connection);

            statement = connection.createStatement();
            logger.info("statement :: [{}]", statement);

            String num = Database.doQueryWithTimeout(statement, 3);
            System.err.println("###########################");
            System.err.println("# entries(): " + num);
            System.err.println("###########################");

        } catch (Exception e) {

            logger.error("Ikkje braaaaa...", e);

        } finally {

            Database.close(rs);
            Database.close(statement);
            Database.close(connection);
            System.exit(0);
        }

    }

}
