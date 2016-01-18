package nl.gogognome.gogohibernate;

import nl.gogognome.dataaccess.DataAccessException;
import org.hibernate.Criteria;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Order;

import java.io.Serializable;
import java.util.List;

public abstract class AbstractHibernateDAO<E> {

    private Session session;
    private final Class<E> entityClass;

    protected AbstractHibernateDAO(Session session, Class<E> entityClass) {
        this.session = session;
        this.entityClass = entityClass;
    }

    public E get(Serializable id) throws DataAccessException {
        try {
            return getSession().get(entityClass, id);
        } catch (Exception e) {
            throw new DataAccessException("Could not get instance of " + entityClass.getName() + " with id " + id, e);
        }
    }

    public Serializable save(E entity) throws DataAccessException {
        try {
            return getSession().save(entity);
        } catch (Exception e) {
            throw new DataAccessException("Could not save entity " + entityClass.getName(), e);
        }
    }

    public boolean exists(Serializable id) throws DataAccessException {
        try {
            Query query = getSession().createQuery("select count(*) from " + entityClass.getSimpleName() + " e where e.id = :id");
            if (id instanceof String) {
                query.setString("id", (String) id);
            } else if (id instanceof Integer) {
                query.setLong("id", (Integer) id);
            } else if (id instanceof Long) {
                query.setLong("id", (Long) id);
            } else {
                throw new DataAccessException("Id of type " + id.getClass().getName() + " is not supported");
            }
            return (Long) query.uniqueResult() > 0;
        } catch (Exception e) {
            throw new DataAccessException("Could not check existance of " + entityClass.getName() + " with id " + id, e);
        }
    }

    protected Session getSession() {
        return session;
    }

    public void update(E entity) throws DataAccessException {
        try {
            if (!session.contains(entity)) {
                getSession().lock(entity, LockMode.NONE);
            }
            session.save(entity);
        } catch (Exception e) {
            throw new DataAccessException("Could not update " + entityClass.getName(), e);
        }
    }

    public void delete(E entity) throws DataAccessException {
        try {
            if (!session.contains(entity)) {
                getSession().lock(entity, LockMode.NONE);
            }
            getSession().delete(entity);
        } catch (Exception e) {
            throw new DataAccessException("Could not delete " + entityClass.getName(), e);
        }
    }

    public List<E> findAll() throws DataAccessException {
        try {
            Criteria c = getSession().createCriteria(entityClass);
            c.addOrder(Order.asc(getIdColumn()));
            return c.list();
        } catch (Exception e) {
            throw new DataAccessException("Could not find all instances of " + entityClass.getName(), e);
        }
    }

    protected abstract Serializable getId(E entity);

    protected abstract String getIdColumn();
}
