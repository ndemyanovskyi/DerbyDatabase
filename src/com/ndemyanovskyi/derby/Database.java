/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.map.Pool;
import com.ndemyanovskyi.map.WeakHashPool;
import com.ndemyanovskyi.throwable.Exceptions;
import com.ndemyanovskyi.throwable.function.ThrowableConsumer;
import com.ndemyanovskyi.throwable.function.ThrowableFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public class Database {
    
    private static final String TABLE_NAMES_QUERY = "select t.tablename from sys.systables t, sys.sysschemas s where t.schemaid = s.schemaid and t.tabletype = 'T'";

    private final Pool<String, Layout> layoutPool = 
	    new WeakHashPool<>(name -> 
		    Utils.createLayout(Database.this, name.toString()));

    private final Connection connection;
    private final String path;

    protected Database(String path, Connection connection) {
	this.connection = Objects.requireNonNull(connection, "connnection");
	this.path = Objects.requireNonNull(path, "path");
    }

    protected Pool<String, Layout> getLayoutPool() {
	return layoutPool;
    }
    
    public List<String> getTables() {
	List<String> tables = new ArrayList<>();
	queryAction(TABLE_NAMES_QUERY, rs -> {
	    while(rs.next()) {
		tables.add(rs.getString("TABLENAME"));
	    }
	});
	return tables;
    }

    public String getPath() {
	return path;
    }

    public Connection getConnection() {
	return connection;
    }

    public Cursor query(String sql) {
	return Cursor.of(this, sql);
    }

    public Cursor query(String sql, Cursor.Type type, Cursor.Concurrency concurrency) {
	return Cursor.of(this, sql, type, concurrency);
    }

    public Cursor query(String sql, Cursor.Concurrency concurrency) {
	return Cursor.of(this, sql, Cursor.Type.defaultValue(), concurrency);
    }

    public Cursor query(String sql, Cursor.Type type) {
	return Cursor.of(this, sql, type, Cursor.Concurrency.defaultValue());
    }

    public Value queryFirst(String sql) {
	return queryAction(sql, rs -> {
	    return new Value(rs.next() ? rs.getObject(1) : null);
	});
    }
    
    public void commit() {
	Exceptions.execute(connection::commit);
    }
    
    public void rollback() {
	Exceptions.execute(() -> connection.rollback());
    }

    public int queryUpdate(String sql) {
	return Exceptions.execute(() -> {
	    Statement s = getConnection().createStatement();
	    int result = s.executeUpdate(sql, Statement.NO_GENERATED_KEYS);
	    s.close();
	    return result;
	});
    }

    public void queryUpdate(String[] sqls) {
	Exceptions.execute(() -> {
	    Statement s = getConnection().createStatement();
	    for(int i = 0; i < sqls.length; i++) {
		s.executeUpdate(sqls[i], Statement.NO_GENERATED_KEYS);
	    }
	    s.close();
	});
    }

    public void queryUpdate(Collection<String> sqls) {
	Exceptions.execute(() -> {
	    Statement s = getConnection().createStatement();
	    for(String sql : sqls) {
		s.executeUpdate(sql, Statement.NO_GENERATED_KEYS);
	    }
	    s.close();
	});
    }

    public void queryPrepared(String sql, ThrowableConsumer<PreparedStatement, SQLException> action) {
	Exceptions.execute(() -> {
	    PreparedStatement s = getConnection().prepareStatement(sql);
	    action.accept(s);
	    s.close();
	});
    }

    public <T> T queryPrepared(String sql, ThrowableFunction<PreparedStatement, T, SQLException> action) {
	return Exceptions.execute(() -> {
	    PreparedStatement s = getConnection().prepareStatement(sql);
	    T result = action.apply(s);
	    s.close();
	    return result;
	});
    }

    public void queryAction(String sql, ThrowableConsumer<ResultSet, SQLException> action) {
	Exceptions.execute(() -> {
	    Statement s = getConnection().createStatement();
	    action.accept(s.executeQuery(sql));
	    s.close();
	});
    }

    public <T> T queryAction(String sql, ThrowableFunction<ResultSet, T, SQLException> action) {
	return Exceptions.execute(() -> {
	    Statement s = getConnection().createStatement();
	    T value = action.apply(s.executeQuery(sql));
	    s.close();
	    return value;
	});
    }

    public void queryForEach(String query, Consumer<Row> action) {
	queryAction(query, rs -> {
	    Row row = new Row(Utils.createLayout(this, rs.getMetaData()));

	    while(rs.next()) {
		for(int i = 0; i <= row.getLayout().size(); i++) {
                    Column column = row.getLayout().get(i); 
		    row.get(column).set(rs.getObject(i + 1), column.getType());
		}
		action.accept(row);
	    }
	});
    }
    
    public static final class Utils extends com.ndemyanovskyi.derby.Utils {
        
        private Utils() {}
        
    }

}
