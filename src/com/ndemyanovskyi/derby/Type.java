/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.collection.list.unmodifiable.UnmodifiableListWrapper;
import java.math.BigDecimal;
import java.sql.Blob;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;

public final class Type<T> {
    
    private static final UnmodifiableListWrapper<Type<?>> values = new UnmodifiableListWrapper<>();

    public static final Type<Long>	    BIG_INT			= new Type<>("BIG_INT", Long.class, false);
    public static final Type<Blob>	    BLOB			= new Type<>("BLOB", Blob.class, true);
    public static final Type<LocalDate>	    DATE			= new Type<>("DATE", LocalDate.class, false);
    public static final Type<BigDecimal>    DECIMAL			= new Type<>("DECIMAL", BigDecimal.class, false);
    public static final Type<Double>	    DOUBLE			= new Type<>("DOUBLE", Double.class, false);
    public static final Type<Double>	    DOUBLE_PRECISION		= new Type<>("DOUBLE_PRECISION", Double.class, false);
    public static final Type<Double>	    FLOAT			= new Type<>("FLOAT", Double.class, false);
    public static final Type<Integer>	    INTEGER			= new Type<>("INTEGER", Integer.class, false);
    public static final Type<String>	    LONG_VARCHAR		= new Type<>("LONG_VARCHAR", String.class, true);
    public static final Type<String>	    LONG_VARCHAR_FOR_BIT_DATA	= new Type<>("LONG_VARCHAR_FOR_BIT_DATA", String.class, true);
    public static final Type<BigDecimal>    NUMERIC			= new Type<>("NUMERIC", BigDecimal.class, false);
    public static final Type<Float>	    REAL			= new Type<>("REAL", Float.class, false);
    public static final Type<Short>	    SMALLINT			= new Type<>("SMALLINT", Short.class, false);
    public static final Type<LocalTime>	    TIME			= new Type<>("TIME", LocalTime.class, false);
    public static final Type<LocalDateTime> TIMESTAMP			= new Type<>("TIMESTAMP", LocalDateTime.class, false);
    public static final Type<String>	    VARCHAR			= new Type<>("VARCHAR", String.class, true);
    public static final Type<String>	    VARCHAR_FOR_BIT_DATA	= new Type<>("VARCHAR_FOR_BIT_DATA", String.class, true);

    private final String name;
    private final Class<T> representClass;
    private final int ordinal;
    private final boolean resizable;

    private Type(String name, Class<T> c, boolean resizable) {
        this.name = Objects.requireNonNull(name);
        this.representClass = Objects.requireNonNull(c);
	this.resizable = resizable;
	values.add(this);
	ordinal = values.size() - 1;
    }
    
    public static <T> Type<T> from(T object) {
	return object != null 
		? from((Class<T>) object.getClass())
		: null;
    }
    
    public static <T> Type<T> from(Class<T> c) {
	if(String.class.isAssignableFrom(c)) {
	    return (Type<T>) VARCHAR;
	}
	
	for(Type<?> type : values) {
	    if(type.getRepresentClass().isAssignableFrom(c)) {
		return (Type<T>) type;
	    }
	}
	
	return null;
    }

    @Override
    public int hashCode() {
	int hash = 5;
	hash = 67 * hash + Objects.hashCode(this.name);
	hash = 67 * hash + Objects.hashCode(this.representClass);
	hash = 67 * hash + (this.resizable ? 1 : 0);
	hash = 67 * hash + this.ordinal;
	return hash;
    }

    @Override
    public boolean equals(Object o) {
	return this == o;
    }

    @Override
    public String toString() {
	return getName() + "<" + getRepresentClass().getSimpleName() + ">";
    }
    
    public static Type<?> of(String name) {
	for(Type<?> type : values()) {
	    if(type.getName().equals(name)) {
		return type;
	    }
	}
	
	return null;
    }

    public int ordinal() {
	return ordinal;
    }

    public String getName() {
        return name;
    }

    public Class<T> getRepresentClass() {
	return representClass;
    }

    public boolean isResizable() {
	return resizable;
    }
    
    public static List<Type<?>> values() {
	return values.unmodifiable();
    }

}
