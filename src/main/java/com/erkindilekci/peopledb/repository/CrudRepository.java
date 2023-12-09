package com.erkindilekci.peopledb.repository;

import com.erkindilekci.peopledb.annotation.Id;
import com.erkindilekci.peopledb.annotation.MultiSQL;
import com.erkindilekci.peopledb.annotation.SQL;
import com.erkindilekci.peopledb.exception.UnableToSaveException;
import com.erkindilekci.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class CrudRepository<T> {

    protected Connection connection;

    public CrudRepository(Connection connection) {
        this.connection = connection;
    }

    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap(msql -> Arrays.stream(msql.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value).findFirst().orElseGet(sqlGetter);
    }

    public T save(T entity) throws UnableToSaveException {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            mapForSave(entity, ps);

            ps.executeUpdate();

            ResultSet rs = ps.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                setIdByAnnotation(entity, id);
                postSave(entity, id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new UnableToSaveException("Tried to save entity: " + entity);
        }

        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
            ps.setLong(1, id);

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();

        try {
            PreparedStatement ps = connection.prepareStatement(
                    getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql),
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entities;
    }

    public long count() {
        long count = 0;

        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.ALL_COUNT, this::getAllCountSql));
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE, this::getDeleteEntitySql));
            ps.setLong(1, getIdByAnnotation(entity));

            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(T... entities) {
        try {
            Statement statement = connection.createStatement();
            String ids = Arrays.stream(entities)
                    .map(this::getIdByAnnotation)
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));

            statement.executeUpdate(getSqlByAnnotation(CrudOperation.DELETE_IN, this::getDeleteInSql).replace(":ids", ids));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found."));
    }

    private void setIdByAnnotation(T entity, Long id) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                });
    }

    public void update(T entity) {
        try {
            PreparedStatement ps = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateEntitySql));
            mapForUpdate(entity, ps);
            ps.setLong(5, getIdByAnnotation(entity));

            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;

    protected void postSave(T entity, long id) {
    }

    protected String getUpdateEntitySql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getDeleteInSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getDeleteEntitySql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getAllCountSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getFindAllSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getSaveSql() {
        throw new RuntimeException("SQL not defined.");
    }

    protected String getFindByIdSql() {
        throw new RuntimeException("SQL not defined.");
    }
}
