package org.brylex.javabin.config;

import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.*;

public class Database {

    private static final Logger logger = LoggerFactory.getLogger(Database.class);

    public static DataSource newDataSource() {

        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:tcp://localhost/~/javaBin;DB_CLOSE_ON_EXIT=FALSE");
        ds.setUser("sa");
        ds.setPassword("");

        Connection connection = null;
        Statement statement = null;
        try {

            connection = ds.getConnection();
            statement = connection.createStatement();

            statement.executeUpdate("create table entry(id number(19,0) primary key, text varchar2(255))");
            statement.executeUpdate("create sequence entry_seq increment by 1");

        } catch (Throwable t) {

            // ignore

        } finally {
            Database.close(statement);
            Database.close(connection);
        }

        return ds;
    }

    public static void sleepSeconds(int seconds) {
        try {
            for (int i=0;i<seconds;i++) {
                Thread.sleep(1000);
                System.err.print(".");
            }
            System.err.println(".");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close(final ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
                logger.info("Closed ResultSet [{}].", resultSet);
            } catch (SQLException e) {
                //
            }
        }
    }

    public static void close(final Statement statement) {
        if (statement != null) {
            try {
                statement.close();
                logger.info("Closed Statement [{}].", statement);
            } catch (SQLException e) {
                //
            }
        }
    }

    public static void close(final Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Closed Connection [{}].", connection);
            } catch (SQLException e) {
                //
            }
        }
    }

    public static String doQuery(final Statement statement) {

        ResultSet rs = null;
        try {

            rs = statement.executeQuery("select count(*) from entry;");
            rs.first();

            return "" + rs.getInt(1);
        } catch (Exception e) {

            throw new RuntimeException(e);

        } finally {
            close(rs);
        }

    }

    public static int doUpdate(final Statement statement) {

            ResultSet rs = null;
            try {

                return statement.executeUpdate("insert into entry(id, text) values (entry_seq.nextval, '" + new Date() + "')");

            } catch (Exception e) {

                throw new RuntimeException(e);

            } finally {
                close(rs);
            }

        }

    public static String doQueryWithTimeout(final Statement statement, final int seconds) throws InterruptedException, ExecutionException, TimeoutException {

        Future<String> future = Executors.newFixedThreadPool(1).submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return doQuery(statement);
            }
        });

        return future.get(seconds, TimeUnit.SECONDS);
    }
}
