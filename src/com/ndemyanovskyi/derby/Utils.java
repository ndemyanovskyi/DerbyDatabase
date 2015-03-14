/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.derby.Column.Builder;
import com.ndemyanovskyi.util.Pair;
import com.ndemyanovskyi.throwable.Exceptions;
import com.ndemyanovskyi.throwable.RuntimeSQLException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class Utils {

    public static Map<String, Pair<Long, Long>> getAutoIncrementValues(Database db, String tableName) {
	return Exceptions.execute(() -> {
	    ResultSet rs = db.getConnection().createStatement(
		    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).
		    executeQuery(
                            "SELECT COLUMNNAME, AUTOINCREMENTSTART, AUTOINCREMENTINC " + 
                            "FROM sys.systables t, sys.syscolumns " +  
                            "WHERE TABLEID = REFERENCEID AND TABLENAME = '" + tableName + "'");
	    Map<String, Pair<Long, Long>> map = new HashMap<>();

	    while(rs.next()) {
		map.put(rs.getString(1),
			new Pair<>(rs.getLong(2), rs.getLong(3)));
	    }
	    rs.close();

	    return map;
	});
    }

    public static Layout createLayout(Database db, String tableName) {
	return Exceptions.execute(() -> {
	    String key = getPrimaryKey(db.getConnection(), tableName);
	    ResultSet rs = db.getConnection().getMetaData().getColumns(null, null, tableName, null);
	    Map<String, Pair<Long, Long>> autoIncValues = null;
	    List<Column> columns = new ArrayList<>();
	    ResultSetMetaData md = rs.getMetaData();
	    Builder builder = Column.builder();

	    while(rs.next()) {
                builder.reset();
		builder.setTableName(tableName);
		builder.setName(rs.getString("COLUMN_NAME"));
		builder.setSize(rs.getInt("COLUMN_SIZE"));
		builder.setType(Type.of(rs.getString("TYPE_NAME")));
		builder.setNullable(rs.getString("IS_NULLABLE").equals("YES"));
		builder.setPrimaryKey(builder.getName().equalsIgnoreCase(key));

		builder.setAutoIncrement(rs.getString("IS_AUTOINCREMENT").equals("YES"));
		builder.setGenerated(rs.getString("IS_GENERATEDCOLUMN").equals("YES"));

		Object def = rs.getObject("COLUMN_DEF");
		if(builder.isAutoIncrement()) {
		    if(autoIncValues == null) {
			autoIncValues = getAutoIncrementValues(db, tableName);
		    }

		    Pair<Long, Long> pair = autoIncValues.get(builder.getName());

		    builder.setAutoIncrementStart(pair.getFirst());
		    builder.setAutoIncrementStep(pair.getSecond());
		    builder.setWritable(!def.toString().startsWith("AUTOINC"));
		} else if(builder.isGenerated()) {
		    builder.setWritable(!def.toString().contains(" ALWAYS "));
		} else {
		    builder.setWritable(true);
		}
		
		builder.setDefaultValue(def);

		columns.add(builder.build());
	    }
	    rs.close();
	    return new Layout(columns);
	});
    }

    public static Layout createLayout(Database db, ResultSetMetaData meta) {
	return Exceptions.execute(() -> {
	    List<Column> columns = new ArrayList<>();
	    
	    for(int i = 1; i <= meta.getColumnCount(); i++) {
		Column column = db.getLayoutPool().get(meta.getTableName(i)).
			getWithName(meta.getColumnName(i));
		columns.add(column.clone());
	    }
	    
	    return new Layout(columns);
	});
    }

    public static String getPrimaryKey(Connection c, String name) {
	return Exceptions.execute(() -> {
	    ResultSet rs = c.getMetaData().getPrimaryKeys(null, null, name);
	    String key = null;
	    
	    if(rs.next()) {
		key = rs.getString("COLUMN_NAME");
	    }
	    rs.close();
	    return key;
	});
    }

    public static List<Pair<String, String>> getPrimaryKeys(Connection c,
							    String... names) {
	if(names == null || names.length == 0) {
	    return getPrimaryKeys(c);
	}

	return getPrimaryKeys(c, Arrays.asList(names));
    }

    public static List<Pair<String, String>> getPrimaryKeys(Connection c,
							    Collection<String> names) {
	if(names == null || names.isEmpty()) {
	    return getPrimaryKeys(c);
	}

	try {
	    List<Pair<String, String>> list = new ArrayList<>(names.size());

	    for(String name : names) {
		ResultSet rs = c.getMetaData().getPrimaryKeys(null, null, name);

		while(rs.next()) {
		    list.add(new Pair<>(rs.getString(2), rs.getString(2)));
		}
		rs.close();
	    }
	    return list;
	} catch(SQLException ex) {
	    throw new RuntimeSQLException(ex);
	}
    }

    public static List<Pair<String, String>> getPrimaryKeys(Connection c) {
	try {
	    List<Pair<String, String>> list = new ArrayList<>();
	    ResultSet tableNames = c.getMetaData().getTables(null, null, "%",
		    null);
	    while(tableNames.next()) {
		ResultSet rs = c.getMetaData().getPrimaryKeys(null, null,
			tableNames.getString(3)); // 3 is TABLE_NAME

		while(rs.next()) {
		    list.add(new Pair<>(rs.getString("TABLE_NAME"), rs
			    .getString("COLUMN_NAME")));
		}
		rs.close();
	    }
	    tableNames.close();
	    return list;
	} catch(SQLException ex) {
	    throw new RuntimeException(ex);
	}
    }

    public static Class<?> classForSQLType(int type) {
	switch(type) {

	    case Types.CHAR:
	    case Types.VARCHAR:
	    case Types.LONGVARCHAR:
		return String.class;

	    case Types.NUMERIC:
	    case Types.DECIMAL:
		return BigDecimal.class;

	    case Types.BIT:
		return Boolean.class;

	    case Types.TINYINT:
		return Byte.class;

	    case Types.SMALLINT:
		return Short.class;

	    case Types.INTEGER:
		return Integer.class;

	    case Types.BIGINT:
		return Long.class;

	    case Types.REAL:
	    case Types.FLOAT:
		return Float.class;

	    case Types.DOUBLE:
		return Double.class;

	    case Types.BINARY:
	    case Types.VARBINARY:
	    case Types.LONGVARBINARY:
		return byte[].class;

	    case Types.DATE:
		return Date.class;

	    case Types.TIME:
		return Time.class;

	    case Types.TIMESTAMP:
		return Timestamp.class;

	    default:
		throw new IllegalArgumentException("Type not supported.");
	}
    }

    public static void resultSetRowForEach(ResultSet rs, Consumer<Object> action) {
	Exceptions.execute(() -> {
	    for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		action.accept(rs.getObject(i));
	    }
	});
    }

    public static void resultSetRowForEach(ResultSet rs,
					   BiConsumer<String, Object> action) {
	Exceptions.execute(() -> {
	    for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		action.accept(rs.getMetaData().getColumnName(i),
			rs.getObject(i));
	    }
	});
    }

    public static void resultSetForEach(ResultSet rs, Runnable action) {
	Exceptions.execute(() -> {
	    if(rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
		rs.beforeFirst();
	    }

	    while(rs.next()) {
		action.run();
	    }
	});
    }
    
    public static SQLException extractCause(Throwable throwable) {
        Objects.requireNonNull(throwable, "throwable");
        
        Throwable cause = throwable.getCause();
        if(cause != null) {
            if(cause instanceof SQLException) {
                SQLException sqlCause = (SQLException) cause;
                SQLException lastSQLException = null;
                Iterator<Throwable> it = sqlCause.iterator();
                while(it.hasNext()) {
                    Throwable next = it.next();
                    if(next instanceof SQLException) {
                        lastSQLException = (SQLException) next;
                    }
                }
                return lastSQLException;
            } else {
                return extractCause(cause);
            }
        } else {
            return null;
        }
    }

}
