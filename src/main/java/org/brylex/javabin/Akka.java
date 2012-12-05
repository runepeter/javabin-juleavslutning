package org.brylex.javabin;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinRouter;
import akka.util.Duration;
import akka.util.FiniteDuration;
import com.jolbox.bonecp.BoneCPDataSource;
import org.brylex.javabin.config.Database;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Akka {

    public static final FiniteDuration ONE_SECOND = Duration.create(1L, TimeUnit.SECONDS);

    public static void main(String[] args) {

        final BoneCPDataSource dataSource = new BoneCPDataSource();
        dataSource.setDatasourceBean(Database.newDataSource());
        dataSource.setAcquireIncrement(1);

        ActorSystem system = ActorSystem.create("javaBin");
        system.actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new LoggingActor(dataSource);
            }
        }).withRouter(new RoundRobinRouter(10)));
    }

    private static class LoggingActor extends UntypedActor {

        private final LoggingAdapter log = Logging.getLogger(context().system(), this);

        private final DataSource dataSource;

        private LoggingActor(final DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public void onReceive(Object message) {

            Connection connection = null;
            Statement statement = null;

            try {
                connection = dataSource.getConnection();
                log.info("connection :: [{}]", connection);

                statement = connection.createStatement();
                log.info("statement :: [{}]", statement);

                Database.doUpdate(statement);
                log.info("Successfully added Entry to database.");

            } catch (Throwable t) {

                log.error(t, "Unable to get Database connection.");

            } finally {
                Database.close(statement);
                Database.close(connection);
            }

            log.info("MSG[{}].", message);
            context().system().scheduler().scheduleOnce(ONE_SECOND, self(), "POLL(" + new Date() + ")");
        }

        @Override
        public void preStart() {
            context().system().scheduler().scheduleOnce(ONE_SECOND, self(), "POLL(" + new Date() + ")");
        }
    }

    private static class DatabaseSpyActor extends UntypedActor {

        private final LoggingAdapter log = Logging.getLogger(context().system(), this);

        private final DataSource dataSource;

        private DatabaseSpyActor(final DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public void onReceive(Object message) {

            Connection connection = null;
            Statement statement = null;

            try {
                connection = dataSource.getConnection();
                log.info("connection :: [{}]", connection);

                statement = connection.createStatement();
                log.info("statement :: [{}]", statement);

                String txt = Database.doQuery(statement);
                System.out.println("There are [" + txt + "] entries() in the database.");

            } catch (Throwable t) {

                // ignore

            } finally {
                Database.close(statement);
                Database.close(connection);
            }

        }

        @Override
        public void preStart() {
            context().system().scheduler().schedule(ONE_SECOND, ONE_SECOND, self(), "POLL(" + new Date() + ")");
        }

    }

}
