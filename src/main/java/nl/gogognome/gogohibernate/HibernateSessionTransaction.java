package nl.gogognome.gogohibernate;

import nl.gogognome.dataaccess.DataAccessException;
import nl.gogognome.dataaccess.transaction.Transaction;
import nl.gogognome.dataaccess.transaction.TransactionSettings;
import nl.gogognome.dataaccess.util.CreationStack;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

class HibernateSessionTransaction implements Transaction {

    private final static Logger LOGGER = LoggerFactory.getLogger(HibernateSessionTransaction.class);

    private final Session session;
    private final CreationStack creationStack;
    private final Connection connection;
    private org.hibernate.Transaction transaction;
    private boolean transactionStarted;

    public HibernateSessionTransaction(Session session, Connection connection) {
        LOGGER.debug("Creating HibernateSessionTransaction");
        creationStack = TransactionSettings.storeCreationStackForTransactions ? new CreationStack() : null;
        this.connection = connection;
        this.session = session;

        ensureTransactionHasBeenStarted();
    }

    public void commit() throws DataAccessException {
        LOGGER.debug("Committing HibernateSessionTransaction");
        try {
            transaction.commit();
        } catch (Exception e) {
            throw new DataAccessException("Failed to commit: " + e.getMessage(), e);
        } finally {
            transactionStarted = false;
        }
    }

    public void rollback() throws DataAccessException {
        LOGGER.debug("Rolling back HibernateSessionTransaction");
        try {
            transaction.rollback();
        } catch (Exception e) {
            throw new DataAccessException("Failed to rollback: " + e.getMessage(), e);
        } finally {
            transactionStarted = false;
        }
    }

    public void close() throws DataAccessException {
        LOGGER.debug("Closing HibernateSessionTransaction");
        try {
            session.close();
            connection.close();
        } catch (Exception e) {
            throw new DataAccessException("Failed to close: " + e.getMessage(), e);
        } finally {
            transactionStarted = false;
        }
    }

    public String getCreationDetails() {
        return creationStack != null ? creationStack.toString() : "Creation stacks are not stored.";
    }

    public void ensureTransactionHasBeenStarted() {
        if (!transactionStarted) {
            transaction = session.beginTransaction();
            transactionStarted = true;
        }
    }
}
