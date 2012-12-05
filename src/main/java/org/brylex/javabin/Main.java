package org.brylex.javabin;

import org.brylex.javabin.config.Database;
import org.brylex.javabin.config.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.brylex.javabin.config.Database.close;

public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        final DataSource dataSource = Database.newDataSource();

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {

                System.err.println("run() :: " + System.currentTimeMillis());

                Connection connection = null;
                Statement statement = null;
                try {

                    connection = dataSource.getConnection();
                    logger.info("Obtained database connection [{}].", connection);

                    statement = connection.createStatement();
                    logger.info("Statement created [{}].", statement);

                    statement.executeUpdate("insert into entry(id, text) values (entry_seq.nextval, '" + new Date() + "')");
                    logger.info("Inserted entry.");

                } catch (Exception e) {

                    e.printStackTrace();

                    logger.error("Unable to insert into database.", e);

                } finally {
                    close(statement);
                    close(connection);
                }

            }
        }, 1, 1, TimeUnit.SECONDS);

    }


}
