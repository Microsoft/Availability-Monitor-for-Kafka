//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.sql;

import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * A lightweight standalone JDBC connection pool manager.
 * <p>The public methods of this class are thread-safe.
 */

public class JdbcConnectionPool {

    final static org.slf4j.Logger m_logger = LoggerFactory.getLogger(JdbcConnectionPool.class);

    /**
     * Object to hold the Connection under the queue, with a TimeStamp.
     */
    private class PCTS implements Comparable<PCTS> {
        private PooledConnection pconn;
        private Calendar timeStamp;

        private PooledConnection getPConn() {
            return this.pconn;
        }

        private Calendar getTimeStamp() {
            return this.timeStamp;
        }

        private PCTS(PooledConnection pconn) {
            this.timeStamp = Calendar.getInstance();
            this.pconn = pconn;
        }

        /*
         * (non-Javadoc)
         *
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        @Override
        public int compareTo(PCTS other) {
            return (int) (other.getTimeStamp().getTimeInMillis() - this
                    .getTimeStamp().getTimeInMillis());
        }
    }

    private class PoolConnectionEventListener implements
            ConnectionEventListener {
        public void connectionClosed(ConnectionEvent event) {
            PooledConnection pconn = (PooledConnection) event.getSource();
            pconn.removeConnectionEventListener(this);
            recycleConnection(pconn);
        }

        public void connectionErrorOccurred(ConnectionEvent event) {
            PooledConnection pconn = (PooledConnection) event.getSource();
            pconn.removeConnectionEventListener(this);
            disposeConnection(pconn);
        }
    }

    /**
     * Looks for recycled connections older than the specified seconds.
     */
    private class ConnectionMonitor extends TimerTask {
        private JdbcConnectionPool owner;

        private ConnectionMonitor(JdbcConnectionPool owner) {
            this.owner = owner;
        }

        @Override
        public void run() {
            Calendar now = Calendar.getInstance();
            synchronized (owner) {
                Iterator<PCTS> iterator = recycledConnections.iterator();
                while (iterator.hasNext()) {
                    PCTS pcts = iterator.next();
                    int delta = (int) ((now.getTimeInMillis() - pcts
                            .getTimeStamp().getTimeInMillis()) / 1000);
                    if (delta >= maxIdleConnectionLife) {
                        closeConnectionNoEx(pcts.getPConn());
                        iterator.remove();
                    }
                }
            }
        }
    }

    /**
     * Thrown in {@link #getConnection()} when no free connection becomes
     * available within <code>timeout</code> seconds.
     */
    public static class TimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1;

        public TimeoutException() {
            super("Timeout while waiting for a free database connection.");
        }
    }

    private ConnectionPoolDataSource dataSource;
    private int maxConnections;
    private int maxIdleConnectionLife;
    private int timeout;
    private Semaphore semaphore;
    private Queue<PCTS> recycledConnections;
    private int activeConnections;

    private PoolConnectionEventListener poolConnectionEventListener;

    private boolean isDisposed;

    private Timer timer;

    /**
     * Constructs a JdbcConnectionPool object with a timeout of 60
     * seconds and 60 seconds of max idle connection life.
     *
     * @param dataSource     the data source for the connections.
     * @param maxConnections the maximum number of connections.
     */
    public JdbcConnectionPool(ConnectionPoolDataSource dataSource,
                              int maxConnections) {
        this(dataSource, maxConnections, 60, 60);
    }

    /**
     * Constructs a JdbcConnectionPool object.
     *
     * @param dataSource            the data source for the connections.
     * @param maxConnections        the maximum number of connections.
     * @param timeout               the maximum time in seconds to wait for a free connection.
     * @param maxIdleConnectionLife the maximum time in seconds to keep an idle connection to wait
     *                              to be used.
     */
    public JdbcConnectionPool(ConnectionPoolDataSource dataSource,
                              int maxConnections, int timeout, int maxIdleConnectionLife) {
        if (dataSource == null)
            throw new InvalidParameterException("dataSource cant be null");
        if (maxConnections < 1)
            throw new InvalidParameterException("maxConnections must be > 1");
        if (timeout < 1)
            throw new InvalidParameterException("timeout must be > 1");
        if (maxIdleConnectionLife < 1)
            throw new InvalidParameterException(
                    "maxIdleConnectionLife must be > 1");

        this.dataSource = dataSource;
        this.maxConnections = maxConnections;
        this.maxIdleConnectionLife = maxIdleConnectionLife;
        this.timeout = timeout;
        semaphore = new Semaphore(maxConnections, true);
        recycledConnections = new PriorityQueue<PCTS>();
        poolConnectionEventListener = new PoolConnectionEventListener();

        // start the monitor
        timer = new Timer(getClass().getSimpleName(), true);
        timer.schedule(new ConnectionMonitor(this), this.maxIdleConnectionLife, this.maxIdleConnectionLife);
    }

    private void assertInnerState() {
        if (activeConnections < 0)
            throw new AssertionError();
        if (activeConnections + recycledConnections.size() > maxConnections)
            throw new AssertionError();
        if (activeConnections + semaphore.availablePermits() > maxConnections)
            throw new AssertionError();
    }

    private void closeConnectionNoEx(PooledConnection pconn) {
        try {
            pconn.close();
        } catch (SQLException e) {
            m_logger.error("Error while closing database connection: " + e.toString(), e);
        }
    }

    /**
     * Closes all unused pooled connections.
     */
    public synchronized void dispose() throws SQLException {
        if (isDisposed)
            return;
        isDisposed = true;
        SQLException e = null;
        while (!recycledConnections.isEmpty()) {
            PCTS pcts = recycledConnections.poll();
            if (pcts == null)
                throw new EmptyStackException();
            PooledConnection pconn = pcts.getPConn();
            try {
                pconn.close();
            } catch (SQLException e2) {
                if (e == null)
                    e = e2;
            }
        }
        timer.cancel();
        if (e != null)
            throw e;
    }

    private synchronized void disposeConnection(PooledConnection pconn) {
        if (activeConnections < 0)
            throw new AssertionError();
        activeConnections--;
        semaphore.release();
        closeConnectionNoEx(pconn);
        assertInnerState();
    }

    /**
     * Returns the number of active (open) connections of this pool. This is the
     * number of <code>Connection</code> objects that have been issued by
     * {@link #getConnection()} for which <code>Connection.close()</code> has
     * not yet been called.
     *
     * @return the number of active connections.
     **/
    public synchronized int getActiveConnections() {
        return activeConnections;
    }

    /**
     * Retrieves a connection from the connection pool. If
     * <code>maxConnections</code> connections are already in use, the method
     * waits until a connection becomes available or <code>timeout</code>
     * seconds elapsed. When the application is finished using the connection,
     * it must close it in order to return it to the pool.
     *
     * @return a new Connection object.
     * @throws TimeoutException when no connection becomes available within
     *                          <code>timeout</code> seconds.
     */
    public Connection getConnection() throws SQLException {
        // This routine is unsynchronized, because semaphore.tryAcquire() may
        // block.
        synchronized (this) {
            if (isDisposed)
                throw new IllegalStateException(
                        "Connection pool has been disposed.");
        }
        try {
            if (!semaphore.tryAcquire(timeout, TimeUnit.SECONDS))
                throw new TimeoutException();
        } catch (InterruptedException e) {
            throw new RuntimeException(
                    "Interrupted while waiting for a database connection.", e);
        }
        boolean ok = false;
        try {
            Connection conn = getConnection2();
            ok = true;
            return conn;
        } finally {
            if (!ok)
                semaphore.release();
        }
    }

    private synchronized Connection getConnection2() throws SQLException {
        if (isDisposed)
            throw new IllegalStateException(
                    "Connection pool has been disposed."); // test again with
        // lock
        PooledConnection pconn;
        if (recycledConnections.size() > 0) {
            PCTS pcts = recycledConnections.poll();
            if (pcts == null)
                throw new EmptyStackException();
            pconn = pcts.getPConn();

        } else {
            pconn = dataSource.getPooledConnection();
        }
        Connection conn = pconn.getConnection();
        activeConnections++;
        pconn.addConnectionEventListener(poolConnectionEventListener);
        assertInnerState();
        return conn;
    }

    private synchronized void recycleConnection(PooledConnection pconn) {
        if (isDisposed) {
            disposeConnection(pconn);
            return;
        }
        if (activeConnections <= 0)
            throw new AssertionError();
        activeConnections--;
        semaphore.release();
        recycledConnections.add(new PCTS(pconn));
        assertInnerState();
    }
}
// end class JdbcConnectionPool