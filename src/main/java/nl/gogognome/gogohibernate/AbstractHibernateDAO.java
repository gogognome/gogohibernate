package nl.gogognome.gogohibernate;

import nl.gogognome.dataaccess.DataAccessException;
import org.hibernate.Criteria;
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

    public E get(String id) {
        return getSession().get(entityClass, id);
    }

    public Serializable save(E entity) {
        return getSession().save(entity);
    }

    public boolean exists(Serializable id) throws DataAccessException {
        Query query = getSession().createQuery("select count(*) from " + entityClass.getSimpleName() + " e where e.id = :id");
        if (id instanceof  String) {
            query.setString("id", (String) id);
        } else if (id instanceof Integer) {
            query.setLong("id", (Integer) id);
        } else if (id instanceof Long) {
            query.setLong("id", (Long) id);
        } else {
            throw new DataAccessException("Id of type " + id.getClass().getName() + " is not supported");
        }
        return (Long) query.uniqueResult() > 0;
    }

    protected Session getSession() {
        return session;
    }

    public E update(E entity) throws DataAccessException {
        if (!exists(getId(entity))) {
            throw new DataAccessException(entityClass.getSimpleName() + ' ' + getId(entity) + " does not exist yet. Only existing entities can be updated.");
        }
        return (E) getSession().merge(entity);
    }

    public void delete(E entity) throws DataAccessException {
        if (!exists(getId(entity))) {
            throw new DataAccessException(entityClass.getSimpleName() + ' ' + getId(entity) + " does not exist. Only existing entities can be deleted.");
        }
        getSession().delete(entity);
    }

    public List<E> findAll() {
        Criteria c = getSession().createCriteria(entityClass);
        c.addOrder(Order.asc(getIdColumn()));
        return c.list();
    }

    protected abstract Serializable getId(E entity);

    protected abstract String getIdColumn();
}
