/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.ndemyanovskyi.derby;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;


public class Row extends HashMap<Column, Value> {
    
    private final Layout layout;

    protected Row(Layout layout) {
	this.layout = layout;
        for(Column c : layout) {
            super.put(c, new Value());
        }
    }

    @Override
    public Layout keySet() {
	return layout;
    }

    @Override
    public Value get(Object key) {
	if(!getLayout().contains(key)) {
	    throw new IllegalArgumentException("Invalid column.");
	}
	
	return super.get(key);
    }

    public Value get(String columnName) {
	return get(layout.getWithName(columnName));
    }

    public Layout getLayout() {
	return layout;
    }

    public Value get(int columnIndex) {
	return get(layout.get(columnIndex));
    }

    @Override
    public Value put(Column key, Value value) {
	throw new UnsupportedOperationException("put");
    }

    /*protected void protectedPut(Column key, Object value) {
	Value v = get(key);
        if(v != null) {
            v.set(value, Type.from(key));
        } else {
            super.put(key, new Value(value));
        }
    }*/

    @Override
    public void clear() {
	throw new UnsupportedOperationException("clear");
    }

    public void reset() {
	values().forEach(e -> e.set(null, e.getType()));
    }

    @Override
    public Value remove(Object key) {
	throw new UnsupportedOperationException("remove");
    }

    @Override
    public Row clone() {
	Row clone = (Row) super.clone();
        for(Entry<Column, Value> e : clone.entrySet()) {
            e.setValue(new Value(e.getValue().get(), e.getKey().getType()));
        }
	return clone;
    }
    
    public Map<Column, Object> getData() {
	Map<Column, Object> data = new HashMap<>();
	for(Entry<Column, Value> e : entrySet()) {
	    data.put(e.getKey(), e.getValue().get());
	}
	return data;
    }
    
    public <T> Map<T, Object> getData(Class<T> keyType) {
	Objects.requireNonNull(keyType, "keyType");
	
	if(Column.class.isAssignableFrom(keyType)) {
	    return (Map<T, Object>) getData();
	} 
	
	if(String.class.isAssignableFrom(keyType)) {
	    Map<String, Object> data = new HashMap<>();
	    for(Entry<Column, Value> e : entrySet()) {
		data.put(e.getKey().getName(), e.getValue().get());
	    }
	    return (Map<T, Object>) data;
	} 
	
	if(Integer.class.isAssignableFrom(keyType)) {
	    Map<String, Object> data = new HashMap<>();
	    for(Entry<Column, Value> e : entrySet()) {
		data.put(e.getKey().getName(), e.getValue().get());
	    }
	    return (Map<T, Object>) data;
	}
	
	throw new IllegalArgumentException(
		"Key type is invalid. Valid types = [Column, String, Integer]");
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        for(Iterator<Column> it = layout.iterator(); it.hasNext();) {
            Column next = it.next();
            b.append(get(next));
            if(it.hasNext()) b.append(", ");
        }
        return b.toString();
    }

}
