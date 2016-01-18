package nl.gogognome.gogohibernate;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.transaction.CompositeTransaction;
import nl.gogognome.dataaccess.transaction.JdbcTransaction;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class HibernateCompositeDatasourceTransaction extends CompositeTransaction implements JdbcTransaction {

    private static final Map<String, SessionFactory> NAME_TO_SESSION_FACTORY = new HashMap<>();
    private static final Map<String, DataSource> NAME_TO_DATA_SOURCE = new HashMap<>();

    private final Map<String, Session> nameToSession = new HashMap<>();
    private final Map<String, Connection> nameToConnection = new HashMap<>();
    private final Map<String, HibernateSessionTransaction> nameToTransaction = new HashMap<>();

    public HibernateCompositeDatasourceTransaction() {
    }

    public static void registerDataSourceAndSessionFactory(String name, DataSource dataSource, SessionFactory sessionFactory) {
        NAME_TO_DATA_SOURCE.put(name, dataSource);
        NAME_TO_SESSION_FACTORY.put(name, sessionFactory);
    }

    public Connection getConnection(Object... parameters) throws SQLException {
        if(parameters.length == 1 && parameters[0] instanceof String) {
            try {
                return this.getConnection((String)parameters[0]);
            } catch (DataAccessException e) {
                if(e.getCause() instanceof SQLException) {
                    throw (SQLException)e.getCause();
                } else {
                    throw new SQLException("Could not get connection: " + e.getMessage(), e);
                }
            }
        } else {
            throw new IllegalArgumentException("Parameter must be an array of length 1 containing a String");
        }
    }

    public Connection getConnection(String datasourceName) throws DataAccessException {
        Connection connection = (Connection)this.nameToConnection.get(datasourceName);
        if(connection != null) {
            nameToTransaction.get(datasourceName).ensureTransactionHasBeenStarted();
            return connection;
        } else {
            if (nameToSession.containsKey(datasourceName)) {
                throw new DataAccessException("Another session for " + datasourceName + " already exists. " +
                        "Close that session first before starting a new session.");
            }
            try {
                connection = NAME_TO_DATA_SOURCE.get(datasourceName).getConnection();
            } catch (SQLException var5) {
                throw new DataAccessException("Failed to get connection from data source " + datasourceName, var5);
            }

            Session session = NAME_TO_SESSION_FACTORY.get(datasourceName).withOptions().connection(connection).openSession();
            nameToSession.put(datasourceName, session);
            nameToConnection.put(datasourceName, connection);
            HibernateSessionTransaction transaction = new HibernateSessionTransaction(session, connection);
            addTransaction(transaction);
            nameToTransaction.put(datasourceName, transaction);

            try {
                if(connection.getTransactionIsolation() != 2) {
                    connection.setTransactionIsolation(2);
                }

                connection.setAutoCommit(false);
                return connection;
            } catch (SQLException var4) {
                throw new DataAccessException("Failed to configure the connection for data source " + datasourceName, var4);
            }
        }
    }

    public Session getSession(String datasourceName) throws DataAccessException {
        if (!nameToSession.containsKey(datasourceName)) {
            getConnection(datasourceName); // ensures session is started
        }
        return nameToSession.get(datasourceName);
    }
}
