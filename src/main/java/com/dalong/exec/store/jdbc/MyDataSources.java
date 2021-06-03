package com.dalong.exec.store.jdbc;

import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.google.common.base.Preconditions;
import java.sql.Driver;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.sql.ConnectionPoolDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;

public final class MyDataSources {
    public enum CommitMode {
        FORCE_AUTO_COMMIT_MODE, FORCE_MANUAL_COMMIT_MODE, DRIVER_SPECIFIED_COMMIT_MODE;
    }

    public static CloseableDataSource newGenericConnectionPoolDataSource(String driver, String url, String username, String password, Properties properties, CommitMode commitMode, int maxIdleConns, long idleTimeSec) {
        Preconditions.checkNotNull(url);
        try {
            Class.forName((String) Preconditions.checkNotNull(driver)).asSubclass(Driver.class);
        } catch (ClassNotFoundException | ClassCastException e) {
            throw new IllegalArgumentException(String.format("String '%s' does not denote a valid java.sql.Driver class name.", new Object[]{driver}), e);
        }
        BasicDataSource source = new BasicDataSource();
        source.setMaxTotal(2147483647);
        source.setTestOnBorrow(true);
        source.setValidationQueryTimeout(1);
        source.setMaxIdle(maxIdleConns);
        source.setSoftMinEvictableIdleTimeMillis(idleTimeSec);
        source.setTimeBetweenEvictionRunsMillis(10000L);
        source.setNumTestsPerEvictionRun(100);
        source.setDriverClassName(driver);
        source.setUrl(url);
        if (properties != null)
            properties.forEach((name, value) -> source.addConnectionProperty(name.toString(), value.toString()));
        if (username != null)
            source.setUsername(username);
        if (password != null)
            source.setPassword(password);
        switch (commitMode) {
            case FORCE_AUTO_COMMIT_MODE:
                source.setDefaultAutoCommit(Boolean.valueOf(true));
                break;
            case FORCE_MANUAL_COMMIT_MODE:
                source.setDefaultAutoCommit(Boolean.valueOf(false));
                break;
        }
        return CloseableDataSource.wrap(source);
    }

    public static CloseableDataSource newSharedDataSource(ConnectionPoolDataSource source, int maxIdleConns, long idleTimeSec) {
        SharedPoolDataSource ds = new SharedPoolDataSource();
        ds.setConnectionPoolDataSource(source);
        ds.setMaxTotal(2147483647);
        ds.setDefaultTestOnBorrow(true);
        ds.setValidationQueryTimeout(1);
        ds.setDefaultMaxIdle(maxIdleConns);
        ds.setDefaultSoftMinEvictableIdleTimeMillis(TimeUnit.SECONDS.toMillis(idleTimeSec));
        ds.setDefaultTimeBetweenEvictionRunsMillis(10000L);
        ds.setDefaultNumTestsPerEvictionRun(100);
        return CloseableDataSource.wrap(ds);
    }
}