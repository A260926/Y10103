package compapp.db;

import compapp.model.Json;

import java.nio.file.Path;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class Store {

    /** A connection whose close() does not throw a checked exception. */
    public interface Tx extends Connection {
        @Override
        void close();
    }

    private final String url;

    public Store(Path dbFile) {
        this.url = "jdbc:sqlite:" + dbFile.toAbsolutePath();
    }

    public Tx open() {
        try {
            Connection conn = DriverManager.getConnection(url);
            try (Statement st = conn.createStatement()) {
                st.execute("PRAGMA foreign_keys = ON");
                st.execute("PRAGMA busy_timeout = 10000");
                st.execute("PRAGMA journal_mode = WAL");
            }
            return new UncheckedConnection(conn);
        } catch (SQLException e) {
            throw new IllegalStateException("cannot open db: " + url, e);
        }
    }

    public void migrate() {
        try (Tx conn = open(); Statement st = conn.createStatement()) {
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS definitions (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      def_key TEXT NOT NULL,
                      version INTEGER NOT NULL,
                      adapter_version TEXT NOT NULL,
                      steps_json TEXT NOT NULL,
                      digest TEXT NOT NULL,
                      created_at_ms INTEGER NOT NULL,
                      UNIQUE(def_key, version)
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS instances (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      def_id INTEGER NOT NULL REFERENCES definitions(id),
                      idem_key TEXT,
                      input_hash TEXT,
                      input_json TEXT NOT NULL,
                      status TEXT NOT NULL,
                      parent_instance_id INTEGER,
                      reuse_note TEXT,
                      lease_owner TEXT,
                      lease_expires_at INTEGER,
                      lease_generation INTEGER NOT NULL DEFAULT 0,
                      comp_order_json TEXT,
                      comp_cursor INTEGER NOT NULL DEFAULT 0,
                      row_version INTEGER NOT NULL DEFAULT 1,
                      created_at_ms INTEGER NOT NULL,
                      updated_at_ms INTEGER NOT NULL
                    )""");
            st.executeUpdate("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_instances_idem
                      ON instances(idem_key) WHERE idem_key IS NOT NULL""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS step_runs (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      instance_id INTEGER NOT NULL REFERENCES instances(id),
                      step_id TEXT NOT NULL,
                      status TEXT NOT NULL,
                      attempt INTEGER NOT NULL DEFAULT 0,
                      input_digest TEXT,
                      executed_generation INTEGER,
                      reused_from_step INTEGER,
                      result_json TEXT,
                      error TEXT,
                      step_version INTEGER NOT NULL DEFAULT 1,
                      UNIQUE(instance_id, step_id)
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS intents (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      step_run_id INTEGER NOT NULL REFERENCES step_runs(id),
                      attempt INTEGER NOT NULL,
                      payload_json TEXT NOT NULL,
                      fencing_token INTEGER NOT NULL,
                      created_at_ms INTEGER NOT NULL
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS receipts (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      step_run_id INTEGER NOT NULL REFERENCES step_runs(id),
                      attempt INTEGER NOT NULL,
                      external_ref TEXT,
                      verdict TEXT NOT NULL,
                      payload_json TEXT,
                      fencing_token INTEGER NOT NULL,
                      accepted INTEGER NOT NULL DEFAULT 1,
                      created_at_ms INTEGER NOT NULL
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS confirms (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      step_run_id INTEGER NOT NULL REFERENCES step_runs(id),
                      attempt INTEGER NOT NULL,
                      verdict TEXT NOT NULL,
                      payload_json TEXT,
                      manual INTEGER NOT NULL DEFAULT 0,
                      operator TEXT,
                      fencing_token INTEGER NOT NULL,
                      created_at_ms INTEGER NOT NULL
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS compensations (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      step_run_id INTEGER NOT NULL REFERENCES step_runs(id),
                      attempt INTEGER NOT NULL,
                      verdict TEXT NOT NULL,
                      payload_json TEXT,
                      fencing_token INTEGER NOT NULL,
                      created_at_ms INTEGER NOT NULL
                    )""");
            st.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS events (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      instance_id INTEGER NOT NULL,
                      step_id TEXT,
                      kind TEXT NOT NULL,
                      detail TEXT,
                      created_at_ms INTEGER NOT NULL
                    )""");
        } catch (SQLException e) {
            throw new IllegalStateException("migration failed", e);
        }
    }

    public void execute(Tx conn, String sql, Object... args) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, args);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("sql failed: " + sql, e);
        }
    }

    public long insert(Tx conn, String sql, Object... args) {
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            bind(ps, args);
            ps.executeUpdate();
            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    return keys.getLong(1);
                }
                throw new IllegalStateException("no generated key");
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insert failed: " + sql, e);
        }
    }

    public int update(Tx conn, String sql, Object... args) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, args);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("update failed: " + sql, e);
        }
    }

    public List<Map<String, Object>> query(Tx conn, String sql, Object... args) {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bind(ps, args);
            try (ResultSet rs = ps.executeQuery()) {
                List<Map<String, Object>> rows = new ArrayList<>();
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= n; i++) {
                        row.put(rs.getMetaData().getColumnLabel(i), rs.getObject(i));
                    }
                    rows.add(row);
                }
                return rows;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("query failed: " + sql, e);
        }
    }

    public Map<String, Object> queryOne(Tx conn, String sql, Object... args) {
        List<Map<String, Object>> rows = query(conn, sql, args);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void bind(PreparedStatement ps, Object... args) throws SQLException {
        for (int i = 0; i < args.length; i++) {
            ps.setObject(i + 1, args[i]);
        }
    }

    private static final class UncheckedConnection implements Tx {
        private final Connection delegate;

        private UncheckedConnection(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override public Statement createStatement() throws SQLException { return delegate.createStatement(); }
        @Override public PreparedStatement prepareStatement(String sql) throws SQLException { return delegate.prepareStatement(sql); }
        @Override public CallableStatement prepareCall(String sql) throws SQLException { return delegate.prepareCall(sql); }
        @Override public String nativeSQL(String sql) throws SQLException { return delegate.nativeSQL(sql); }
        @Override public void setAutoCommit(boolean autoCommit) throws SQLException { delegate.setAutoCommit(autoCommit); }
        @Override public boolean getAutoCommit() throws SQLException { return delegate.getAutoCommit(); }
        @Override public void commit() throws SQLException { delegate.commit(); }
        @Override public void rollback() throws SQLException { delegate.rollback(); }
        @Override public boolean isClosed() throws SQLException { return delegate.isClosed(); }
        @Override public java.sql.DatabaseMetaData getMetaData() throws SQLException { return delegate.getMetaData(); }
        @Override public void setReadOnly(boolean readOnly) throws SQLException { delegate.setReadOnly(readOnly); }
        @Override public boolean isReadOnly() throws SQLException { return delegate.isReadOnly(); }
        @Override public void setCatalog(String catalog) throws SQLException { delegate.setCatalog(catalog); }
        @Override public String getCatalog() throws SQLException { return delegate.getCatalog(); }
        @Override public void setTransactionIsolation(int level) throws SQLException { delegate.setTransactionIsolation(level); }
        @Override public int getTransactionIsolation() throws SQLException { return delegate.getTransactionIsolation(); }
        @Override public SQLWarning getWarnings() throws SQLException { return delegate.getWarnings(); }
        @Override public void clearWarnings() throws SQLException { delegate.clearWarnings(); }
        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.createStatement(resultSetType, resultSetConcurrency); }
        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency); }
        @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException { return delegate.prepareCall(sql, resultSetType, resultSetConcurrency); }
        @Override public Map<String, Class<?>> getTypeMap() throws SQLException { return delegate.getTypeMap(); }
        @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException { delegate.setTypeMap(map); }
        @Override public void setHoldability(int holdability) throws SQLException { delegate.setHoldability(holdability); }
        @Override public int getHoldability() throws SQLException { return delegate.getHoldability(); }
        @Override public Savepoint setSavepoint() throws SQLException { return delegate.setSavepoint(); }
        @Override public Savepoint setSavepoint(String name) throws SQLException { return delegate.setSavepoint(name); }
        @Override public void rollback(Savepoint savepoint) throws SQLException { delegate.rollback(savepoint); }
        @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException { delegate.releaseSavepoint(savepoint); }
        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException { return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability); }
        @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException { return delegate.prepareStatement(sql, autoGeneratedKeys); }
        @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException { return delegate.prepareStatement(sql, columnIndexes); }
        @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException { return delegate.prepareStatement(sql, columnNames); }
        @Override public Clob createClob() throws SQLException { return delegate.createClob(); }
        @Override public java.sql.Blob createBlob() throws SQLException { return delegate.createBlob(); }
        @Override public NClob createNClob() throws SQLException { return delegate.createNClob(); }
        @Override public SQLXML createSQLXML() throws SQLException { return delegate.createSQLXML(); }
        @Override public boolean isValid(int timeout) throws SQLException { return delegate.isValid(timeout); }
        @Override public void setClientInfo(String name, String value) throws SQLClientInfoException { delegate.setClientInfo(name, value); }
        @Override public void setClientInfo(Properties properties) throws SQLClientInfoException { delegate.setClientInfo(properties); }
        @Override public String getClientInfo(String name) throws SQLException { return delegate.getClientInfo(name); }
        @Override public Properties getClientInfo() throws SQLException { return delegate.getClientInfo(); }
        @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException { return delegate.createArrayOf(typeName, elements); }
        @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException { return delegate.createStruct(typeName, attributes); }
        @Override public void setSchema(String schema) throws SQLException { delegate.setSchema(schema); }
        @Override public String getSchema() throws SQLException { return delegate.getSchema(); }
        @Override public void abort(Executor executor) throws SQLException { delegate.abort(executor); }
        @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException { delegate.setNetworkTimeout(executor, milliseconds); }
        @Override public int getNetworkTimeout() throws SQLException { return delegate.getNetworkTimeout(); }
        @Override public <T> T unwrap(Class<T> iface) throws SQLException { return delegate.unwrap(iface); }
        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return delegate.isWrapperFor(iface); }
    }
}
