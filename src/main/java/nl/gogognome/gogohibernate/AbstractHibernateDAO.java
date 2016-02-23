package nl.gogognome.gogohibernate;

import nl.gogognome.dataaccess.DataAccessException;
import org.hibernate.Criteria;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Order;

import java.io.Serializable;
import java.lang.reflect.Method;
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

    public Serializable create(E entity) throws DataAccessException {
        if (exists(getId(entity))) {
            throw new DataAccessException("Cannot create " + entityClass.getName() + " with id " + getId(entity)
                    + " because it has already been persisted in the database before");
        }
        try {
            return getSession().save(entity);
        } catch (Exception e) {
            throw new DataAccessException("Could not save entity " + entityClass.getName(), e);
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
        if (id == null) {
            return false;
        }
        try {
            Query query = getSession().createQuery("select count(*) from " + entityClass.getSimpleName() + " e where e." + getIdColumn() + " = :id");
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
        if (!exists(getId(entity))) {
            throw new DataAccessException("Cannot update " + entityClass.getName() + " with id " + getId(entity)
                    + " because it has not been persisted in the database before");
        }
        try {
            session.saveOrUpdate(entity);
        } catch (Exception e) {
            throw new DataAccessException("Could not update " + entityClass.getName(), e);
        }
    }

    public void deleteById(Serializable id) throws DataAccessException {
        try {
            getSession().createQuery("delete from " + entityClass.getSimpleName() + " where " + getIdColumn() + "=:id")
                    .setParameter("id", id)
                    .executeUpdate();
        } catch (Exception e) {
            throw new DataAccessException("Could not delete " + entityClass.getName() + " with id " + id, e);
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

    public void flush() throws DataAccessException {
        try {
            getSession().flush();
        } catch (Exception e) {
            throw new DataAccessException("Could not find all instances of " + entityClass.getName(), e);
        }
    }

    /**
     * Gets the id of the entity. This implementation returns the value of the getId() method of the entitiy.
     * If the entity has no getId() method, then override this method and return the identity value for the entity parameter.
     * @param entity an entity object
     * @return the id of the entity object
     */
    protected Serializable getId(E entity) {
        if (entity == null) {
            throw new IllegalArgumentException("Cannot get id from null.");
        }
        try {
            Method getId = entity.getClass().getMethod("getId");
            return (Serializable) getId.invoke(entity);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not get id from entity object with type " + entity.getClass().getName());
        }
    }

    /**
     * Override this method if the database column that contains the primary key differs from "id".
     * @return the database column that contains the primary key
     */
    protected String getIdColumn() {
        return "id";
    }
}
