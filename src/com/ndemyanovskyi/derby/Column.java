/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.util.number.Numbers.Longs;
import java.util.Objects;

/**
 *
 * @author Назарій
 */
public class Column implements Cloneable {
    
    //public static final Column COUNT_COLUMN = builder().setName("COUNT").setType(Type.INTEGER).build();

    private final String tableName;
    private String string;
    private Layout layout;
    private final int size;
    private final String name;
    private final Type<?> type;
    private final long autoIncrementStep, autoIncrementStart;
    private final Object defaultValue;
    private final boolean primaryKey;
    private final boolean autoIncrement;
    private final boolean nullable;
    private final boolean writable;
    private final boolean generated;

    private Column(Builder builder) {
        this.autoIncrementStart = builder.getAutoIncrementStart();
        this.autoIncrementStep = builder.getAutoIncrementStep();
        this.autoIncrement = builder.isAutoIncrement();
        this.defaultValue = builder.getDefaultValue();
        this.tableName = builder.getTableName();
        this.generated = builder.isGenerated();
        this.primaryKey = builder.isPrimary();
        this.writable = builder.isWritable();
        this.nullable = builder.isNullable();
        this.type = builder.getType();
        this.name = builder.getName();
        this.size = builder.getSize();
        
        Builder.checkSetted(this.name, "Name");
        Builder.checkSetted(this.tableName, "Table name");
        Builder.checkSetted(this.type, "Type");
        Builder.checkSize(this.size);
    }

    public String getTableName() {
	return tableName;
    }

    @Override
    public String toString() {
	if(string == null) {
	    string = updateString();
	}
	return string;
    }

    public String getName() {
	return name;
    }

    public boolean isReadOnly() {
	return !isWritable();
    }

    public boolean isResizable() {
	return getType().isResizable();
    }

    public int getSize() {
	return size;
    }

    public Type<?> getType() {
	return type;
    }

    public Layout getLayout() {
	return layout;
    }

    protected void setLayout(Layout layout) {
	this.layout = layout;
    }

    public boolean isPrimaryKey() {
	return primaryKey;
    }

    public boolean isAutoIncrement() {
	return autoIncrement;
    }

    public boolean isNullable() {
	return nullable;
    }

    public boolean isWritable() {
	return writable;
    }

    public boolean isGenerated() {
	return generated;
    }

    public Object getDefaultValue() {
	return defaultValue;
    }

    @Override
    public Column clone() {
	Column clone = null;
	try {
	    clone = (Column) super.clone();
	} catch(CloneNotSupportedException cns) {
	    throw new RuntimeException(cns);
	}

	clone.layout = null;
	return clone;
    }

    @Override
    public int hashCode() {
	int hash = 7;
	
	hash = 71 * hash + Objects.hashCode(this.name);
	hash = 71 * hash + Objects.hashCode(this.type);
	hash = 71 * hash + Objects.hashCode(this.defaultValue);
	
	hash = 71 * hash + this.size;
	hash = 71 * hash + (this.nullable ? 1 : 0);
	hash = 71 * hash + (this.writable ? 1 : 0);
	hash = 71 * hash + (this.generated ? 1 : 0);
	hash = 71 * hash + (this.primaryKey ? 1 : 0);
	hash = 71 * hash + (this.autoIncrement ? 1 : 0);
	
	hash = 71 * hash + (int) (this.autoIncrementStep ^ (this.autoIncrementStep >>> 32));
	hash = 71 * hash + (int) (this.autoIncrementStart ^ (this.autoIncrementStart >>> 32));
	
	return hash;
    }

    @Override
    public boolean equals(Object obj) {
	if(obj == null || !(obj instanceof Column)) {
	    return false;
	}
	
	final Column other = (Column) obj;
	
	if(this.size != other.size) return false;
	if(this.nullable != other.nullable) return false;
	if(this.writable != other.writable) return false;
	if(this.generated != other.generated) return false;
	if(this.primaryKey != other.primaryKey) return false;
	if(this.autoIncrement != other.autoIncrement) return false;
	if(this.autoIncrementStep != other.autoIncrementStep) return false;
	if(this.autoIncrementStart != other.autoIncrementStart) return false;
	
	if(!Objects.equals(this.name, other.name)) return false;
	if(!Objects.equals(this.type, other.type)) return false;
	if(!Objects.equals(this.layout, other.layout)) return false;
	if(!Objects.equals(this.defaultValue, other.defaultValue)) return false;
	
	return true;
    }

    private String updateString() {
	StringBuilder b = new StringBuilder();
	b.append(getName()).append(' ');
	b.append(getType().getName());
	b.append(isResizable() ? "(" + getSize() + ")" : "");
	b.append(isNullable() ? "" : " NOT NULL");
	b.append(isPrimaryKey() ? " PRIMARY KEY" : "");

	if(isAutoIncrement()) {
	    b.append(" GENERATED ");

	    String def = getDefaultValue().toString();
	    b.append(isReadOnly() ? "ALWAYS" : "BY DEFAULT");
	    b.append(" AS IDENTITY (START WITH ").append(getAutoIncrementStart());
	    b.append(", INCREMENT BY ").append(getAutoIncrementStep()).append(')');
	} else {
	    if(isGenerated()) {
		b.append(' ').append(getDefaultValue());
	    } else {
		b.append(getDefaultValue() != null ? " DEFAULT " + getDefaultValue() : "");
	    }
	}

	return b.toString();
    }

    public long getAutoIncrementStep() {
	return autoIncrementStep;
    }

    public long getAutoIncrementStart() {
	return autoIncrementStart;
    }

    public static Builder builder() {
	return new Builder();
    }

    public static class Builder {

	private String name;
	private Type<?> type;

	private int size;
	private String tableName;
	private Object defaultValue;
	private boolean primary;
	private boolean autoIncrement;
	private long autoIncrementStep, autoIncrementStart;
	private boolean nullable;
	private boolean writable;
	private boolean generated;

	private Builder() {
            reset();
	}
	
	private static void checkSetted(Object value, String name) {
	    if(value == null) {
		throw new IllegalStateException(
			"Column can`t be build. " + name + " must be setted.");
	    }
	}
        
        private static void checkSize(int size) {
            if(size <= 0) {
                throw new IllegalArgumentException(
                        "size(" + size + ") must be greater then 0.");
            }
        }

	public Column build() {
	    return new Column(this);
	}
        
        public Builder reset() {
            name = null;
            type = null;
            tableName = null;
            autoIncrement = false;
            autoIncrementStart = 0;
            autoIncrementStep = 0;
            defaultValue = null;
            generated = false;
            nullable = true;
            primary = false;
            writable = true;
            size = 0;
            return this;
        }

	//<editor-fold defaultstate="collapsed" desc="Getters and setters">

	public Builder setTableName(String tableName) {
	    this.tableName = Objects.requireNonNull(tableName);
	    return this;
	}

	public String getTableName() {
	    return tableName;
	}
		
	public Builder setName(String name) {
	    this.name = Objects.requireNonNull(name);
	    return this;
	}

	public int getSize() {
	    return size;
	}

	public Builder setSize(int size) {
            checkSize(size);
	    this.size = size;
	    return this;
	}

	public Builder setType(Type<?> type) {
	    this.type = Objects.requireNonNull(type);
	    return this;
	}

	public void setDefaultValue(Object defaultValue) {
	    this.defaultValue = defaultValue;
	}

	public Builder setGenerated(boolean generated) {
	    this.generated = generated;
	    return this;
	}

	public boolean isGenerated() {
	    return generated;
	}

	public String getName() {
	    return name;
	}

	public Type<?> getType() {
	    return type;
	}

	public Object getDefaultValue() {
	    return defaultValue;
	}

	public Builder setWritable(boolean writable) {
	    this.writable = writable;
	    return this;
	}

	public boolean isPrimary() {
	    return primary;
	}

	public boolean isAutoIncrement() {
	    return autoIncrement;
	}

	public Builder setAutoIncrementStep(long autoIncrementStep) {
	    this.autoIncrementStep = Longs.requireInRange(
		    autoIncrementStep, 0, Long.MAX_VALUE);
	    return this;
	}

	public Builder setAutoIncrementStart(long autoIncrementStart) {
	    this.autoIncrementStart = Longs.requireInRange(
		    autoIncrementStart, 0, Long.MAX_VALUE);
	    return this;
	}

	public long getAutoIncrementStep() {
	    return autoIncrementStep;
	}

	public long getAutoIncrementStart() {
	    return autoIncrementStart;
	}

	public boolean isNullable() {
	    return nullable;
	}

	public boolean isWritable() {
	    return writable;
	}

	public Builder setPrimaryKey(boolean primary) {
	    this.primary = primary;
	    return this;
	}

	public Builder setAutoIncrement(boolean autoIncrement) {
	    this.autoIncrement = autoIncrement;
	    return this;
	}

	public Builder setNullable(boolean nullable) {
	    this.nullable = nullable;
	    return this;
	}

	/*public void setWritable(boolean writable) {
	 this.writable = writable;
	 }*/
	public Builder setAll(Column c) {
	    autoIncrementStart = c.getAutoIncrementStart();
	    autoIncrementStep = c.getAutoIncrementStep();
	    autoIncrement = c.isAutoIncrement();
	    defaultValue = c.getDefaultValue();
	    writable = c.isWritable();
	    nullable = c.isNullable();
	    primary = c.isPrimaryKey();
	    type = c.getType();
	    name = c.getName();
	    size = c.getSize();
	    return this;
	}

	public Builder setAll(Builder b) {
	    autoIncrement = b.autoIncrement;
	    defaultValue = b.defaultValue;
	    writable = b.writable;
	    nullable = b.nullable;
	    primary = b.primary;
	    type = b.type;
	    name = b.name;
	    return this;
	}
	//</editor-fold>

    }

}
