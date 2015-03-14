/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.collection.list.ConvertedList;
import com.ndemyanovskyi.collection.list.IndexList;
import com.ndemyanovskyi.util.BiConverter;
import com.ndemyanovskyi.util.Converter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 *
 * @author Андріан
 */
public class Layout extends ArrayList<Column> implements Set<Column> {

    private Cursor cursor;

    private IndexList<Column> modifiableColumns;
    private IndexList<Column> unmodifiableColumns;
    private IndexList<Column> primaryKeys;
    
    private List<String> columnNames;

    protected Layout() {
    }

    public Layout(Collection<Column> columns) {
	for (Column c : columns) {
	    protectedAdd(c);
	}
    }
    
    public List<String> getColumnNames() {
        return columnNames != null ? columnNames 
                : (columnNames = new ConvertedList<>(this, String.class,
                        BiConverter.of(Converter.unsupported(), c -> c.getName()))); 
    }

    public List<Column> getModifiableColumns() {
	if (modifiableColumns == null) {
	    modifiableColumns = new IndexList<>(Layout.this);
	}

	return modifiableColumns;
    }

    public List<Column> getUnmodifiableColumns() {
	if (unmodifiableColumns == null) {
	    unmodifiableColumns = new IndexList<>(Layout.this);
	}

	return unmodifiableColumns;
    }

    public List<Column> getPrimaryKeys() {
	if (primaryKeys == null) {
	    primaryKeys = new IndexList<>(Layout.this);
	}

	return primaryKeys;
    }

    protected void setCursor(Cursor cursor) {
	this.cursor = cursor;
    }

    public Cursor getCursor() {
	return cursor;
    }


    protected void protectedAdd(Column c) {
	protectedAdd(size(), c);
    }

    @Override
    public Layout clone() {
	Layout clone = (Layout) super.clone();
	clone.protectedClear();
	for (Column c : this) {
	    clone.protectedAdd(c.clone());
	}
	return clone;
    }

    protected void protectedClear() {
	super.clear();
    }

    protected boolean protectedAdd(int index, Column column) {
	if(column.getLayout() != null) {
	    throw new IllegalArgumentException(
		    "Column can`t be added. Column contains in other layout.");
	}

	/*if (column.isPrimaryKey()) {
	    ((IndexList) getPrimaryKeys()).
		    getAdapter().getIndexes().add(index);
	}*/

	super.add(index, column);
	column.setLayout(this);

	/*if (column.isWritable()) {
	    ((IndexList) getModifiableColumns()).
		    getAdapter().getIndexes().add(index);
	} else {
	    ((IndexList) getUnmodifiableColumns()).
		    getAdapter().getIndexes().add(index);
	}*/

	return true;
    }

    public Column getWithName(String name) {
	for (Column c : this) {
	    if (c.getName().equals(name)) {
		return c;
	    }
	}
	return null;
    }

    protected Column getWithNameOrThrow(String name) {
	Column c = getWithName(name);
	if(c == null) {
	    throw new IllegalArgumentException(
		    "Column with name '" + name + "' not contains.");
	}
	return c;
    }

    public int indexOfWithName(String name) {
	return indexOf(getWithName(name));
    }

    public int lastIndexOfWithName(String name) {
	return lastIndexOf(getWithName(name));
    }

    public boolean containsWithName(String name) {
	return contains(getWithName(name));
    }

    @Override
    public boolean add(Column e) {
	throw new UnsupportedOperationException("add");
    }

    @Override
    public boolean addAll(Collection<? extends Column> c) {
	throw new UnsupportedOperationException("addAll");
    }

    @Override
    public void clear() {
	throw new UnsupportedOperationException("clear");
    }

    @Override
    public boolean remove(Object o) {
	throw new UnsupportedOperationException("remove");
    }

    @Override
    public Column remove(int index) {
	throw new UnsupportedOperationException("remove");
    }

    @Override
    public Column set(int index, Column element) {
	throw new UnsupportedOperationException("set");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
	throw new UnsupportedOperationException("removeAll");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
	throw new UnsupportedOperationException("retainAll");
    }

    @Override
    public boolean removeIf(Predicate<? super Column> filter) {
	throw new UnsupportedOperationException("removeIf");
    }

    @Override
    public void replaceAll(UnaryOperator<Column> operator) {
	throw new UnsupportedOperationException("replaceAll");
    }

    @Override
    public int hashCode() {
	int hash = 1;
	for (Column column : this) {
	    hash = 31 * hash + Objects.hashCode(column);
	}
	return hash;
    }

    @Override
    public boolean equals(Object object) {
	if (object == this) {
	    return true;
	}

	if (object == null) {
	    return false;
	}
	if (!(object instanceof Layout)) {
	    return false;
	}
	Layout b = (Layout) object;
	return size() == b.size() && containsAll(b);
    }

    @Override
    public String toString() {
	int size = size();

	switch (size) {
	    case 0:
		return "";
	    case 1:
		return get(0).toString();

	    default:
		StringBuilder b = new StringBuilder(size * 10);
		Iterator<Column> it = iterator();
		while (it.hasNext()) {
		    b.append(it.next());
		    b.append(it.hasNext() ? ", " : "");
		}
		return b.toString();
	}
    }

}
