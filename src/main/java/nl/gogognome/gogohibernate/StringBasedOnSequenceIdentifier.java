package nl.gogognome.gogohibernate;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.id.IdentifierGenerator;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StringBasedOnSequenceIdentifier implements IdentifierGenerator {

    @Override
    public Serializable generate(SessionImplementor session, Object object) throws HibernateException {
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = session.connection().prepareStatement("select " + getSequenceName() + ".nextval from dual");
            ResultSet result = preparedStatement.executeQuery();
            if (result.next()) {
                return Long.toString(result.getLong(1));
            }
            return null;
        } catch (SQLException e) {
            throw new HibernateException("Failed to generate sequence for instance of " + object.getClass().getName(), e);
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new HibernateException(e);
                }
            }
        }
    }

    /**
     * Override this method to override the sequence name. The default implementation returns "hibernate_sequence".
     * @return the sequence name
     */
    protected String getSequenceName() {
        return "hibernate_sequence";
    }
}
