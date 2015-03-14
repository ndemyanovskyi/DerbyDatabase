/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 *
 * @author Назарій
 */
public class Queries {

    private Queries() {}
    
    public static String select(String table, String codition) {
	Objects.requireNonNull(table, "table");
	Objects.requireNonNull(codition, "condition");
	return "select * from " + table + " where " + codition;
    }
    
    public static String count(String sql) {
	String lowerSql = sql.toLowerCase();
	StringBuilder b = new StringBuilder(sql);
	b.replace(lowerSql.indexOf("select ") + 7, lowerSql.indexOf(" from"), "count(*)");
	return b.toString();
    }
    
    public static String select(String table, Collection<Column> columns) {
	if(columns.isEmpty()) {
	    throw new IllegalArgumentException(
		    "Column collection can`t be empty.");
	}
	
	StringBuilder b = new StringBuilder();
	b.append("select ");
	for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
	    b.append(it.next().getName());
	    if(it.hasNext()) b.append(", ");
	} 
	b.append(" from ").append(table);
	return b.toString();
    }
    
    public static String select(String table, String condition, Collection<Column> columns) {
	return select(table, columns) + " where " + condition;
    }
    
    public static String alter(String table, Column c, Type<?> type) {
        return "alter table " + table + " alter column " + c.getName() + ' ' + type.getName();
    }
    
    public static String add(String table, Column c) {
        return "alter table " + table + " add column " + c.toString();
    }
    
    public static String delete(String table, Column c) {
        return "alter table " + table + " delete column " + c.toString();
    }
    
    public static String select(String table) {
        return "select * from " + table;
    }
    
    public static String drop(String table) {
        return "drop table " + table;
    }
    
    public static String drop(String table, Column column) {
        return "alter table " + table + " drop column " + column.getName();
    }

    public static String create(String table, Layout layout) {
        return "create table " + table + " (" + layout + ")";
    }

    public static String create(String table, String layout) {
        return "create table " + table + " (" + layout + ")";
    }

    public static <V> String insert(String table, Map<Column, V> row) {
        StringBuilder b = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        b.append("insert into ").append(table);
        Iterator<Entry<Column, V>> it = row.entrySet().iterator();  
        while(it.hasNext()) {
            Entry<Column, ?> e = it.next();
            if (!e.getKey().isAutoIncrement()) {
                if (values.length() > 0) {
                    columns.append(", ");
                    values.append(", ");
                } else {
                    columns.append(" (");
                    values.append(" values (");
                }
                
                columns.append(e.getKey().getName());
                if (e.getKey().getType().getRepresentClass() == String.class) {
                    values.append('\'').append(e.getValue()).append('\'');
                } else {
                    values.append(e.getValue());
                }
            }
        }
        
        b.append(columns.toString()).append(") ").append(values.toString()).append(")");

        /*Set<Column> columns = row.keySet();
         b.append(" (");
         for(Iterator<Entry<Column, Object>> it = row.entrySet().iterator(); it.hasNext(); ) {
            
         Object o = it.next();
         b.append('\'').append(o).append('\'');
         b.append(it.hasNext() ? ", " : " ");            
         }
         b.append(") ");
        
         b.append(" values (");
         for(Iterator it = row.values().iterator(); it.hasNext(); ) {
         Object o = it.next();
         b.append('\'').append(o).append('\'');
         b.append(it.hasNext() ? ", " : " ");            
         }*/
        return b.toString();
    }

    public static String insert(String table, Collection<Map<Column, ?>> rows) {
        StringBuilder b = new StringBuilder();
        b.append("insert into ").append(table);

        b.append(" values ");
        for (Iterator<Map<Column, ?>> rowIt = rows.iterator(); rowIt.hasNext();) {
            b.append(" (");
            for (Iterator it = rowIt.next().values().iterator(); it.hasNext();) {
                Object o = it.next();
                b.append('\'').append(o).append('\'');
                b.append(it.hasNext() ? ", " : "");
            }
            b.append(rowIt.hasNext() ? "), " : ")");
        }

        return b.toString();
    }

    /*public static String delete(Map<Column, ?> row, String table) {
        StringBuilder b = new StringBuilder();
        b.append("delete from ").append(table.getName());
        b.append(" where ").append(row.getCondition());
        return b.toString();
    }*/

    public static String truncate(String table) {
        return "truncate table " + table;
    }

    /*public static String update(Map<? extends Column, ?> values, Row row, Table table) {
        StringBuilder b = new StringBuilder();
        b.append("update top(1) ");
        b.append(table.getName());
        b.append(" set ");

        for (Iterator it = values.entrySet().iterator(); it.hasNext();) {
            Entry<? extends Column, ?> next = (Entry<? extends Column, ?>) it.next();
            b.append(next.getKey().getName()).append(" ").append('\'').append(next.getValue()).append('\'');
            b.append(it.hasNext() ? ", " : " ");
        }

        b.append(" where ").append(row.getCondition());
        return b.toString();
    }

    public static String update(Collection<?> values, Row row, Table table) {
        StringBuilder b = new StringBuilder();
        b.append("update top(1) ");
        b.append(table.getName());
        b.append(" set ");

        int count = 0;
        for (Iterator<?> it = values.iterator(); it.hasNext();) {
            b.append(row.getLayout().get(count)).append(" \'").append(it.next()).append('\'');
            b.append(it.hasNext() ? ", " : " ");
        }

        b.append(" where ").append(row.getCondition());
        return b.toString();
    }

    public static String update(Column column, Object value, Row row, Table table) {
        StringBuilder b = new StringBuilder();
        b.append("update ");
        b.append(table.getName());
        b.append(" set ");

        if (column.getType().asClass() == String.class) {
            b.append(column.getName()).append(" = \'").append(value).append('\'');
        } else {
            b.append(column.getName()).append(" = ").append(value);
        }

        b.append(" where ").append(row.getCondition());
        return b.toString();
    }

    public static String update(Entry<? extends Column, ?> entry, Row row, Table table) {
        return update(entry.getKey(), entry.getValue(), row, table);
    }*/

}
