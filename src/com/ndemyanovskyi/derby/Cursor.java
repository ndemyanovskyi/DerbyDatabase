/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.throwable.Exceptions;
import java.io.Closeable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

public abstract class Cursor implements Iterable<Row>, Closeable {

    public enum Type {

	FORWARD_ONLY(ResultSet.TYPE_FORWARD_ONLY),
	SCROLL_SENSITIVE(ResultSet.TYPE_SCROLL_SENSITIVE),
	SCROLL_INSENSITIVE(ResultSet.TYPE_SCROLL_INSENSITIVE);

	private final int id;

	private Type(int id) {
	    this.id = id;
	}

	public int id() {
	    return id;
	}

	public static Type defaultValue() {
	    return FORWARD_ONLY;
	}

    }

    public enum Concurrency {

	UPDATABLE(ResultSet.CONCUR_UPDATABLE),
	READ_ONLY(ResultSet.CONCUR_READ_ONLY);

	private final int id;

	private Concurrency(int id) {
	    this.id = id;
	}

	public int id() {
	    return id;
	}

	public static Concurrency defaultValue() {
	    return READ_ONLY;
	}

    }

    public enum State {

	UPDATED, UPDATING, /*REMOVED*/;

    }

    public static final int BEGIN_POSITION = -1;
    public static final int UNKNOWN_COUNT = -1;

    private final Type type;
    private final Concurrency concurrency;
    private Value value;

    private Database database;
    private Layout layout;
    private State state = State.UPDATED;
    private ResultSet resultSet;
    private String sql;

    protected int count = UNKNOWN_COUNT;

    private Cursor(Database database, String sql, Type type, Concurrency concurrency) {
	this.database = Objects.requireNonNull(database);
	this.type = Objects.requireNonNull(type);
	this.concurrency = Objects.requireNonNull(concurrency);
	this.sql = Objects.requireNonNull(sql);
    }
    
    protected static Cursor of(Database database, String sql) {
	return of(database, sql, Type.defaultValue(), Concurrency.defaultValue());
    }
    
    protected static Cursor of(Database database, String sql, Type type, Concurrency concurrency) {
	return (type == Type.FORWARD_ONLY) ? 
		new ForwardCursor(database, sql, concurrency) : 
		new ScrollableCursor(database, sql, type, concurrency);
    }

    public final void init() {
	if(resultSet == null) {
	    Exceptions.execute(() -> {
		resultSet = database.getConnection().
                        createStatement(type.id(), concurrency.id()).executeQuery(sql);
		layout = Utils.createLayout(database, resultSet.getMetaData());
		value = new Value();
	    });
	}
    }

    protected ResultSet getResultSet() {
	init();
	return resultSet;
    }

    public String getSQL() {
	return sql;
    }

    public Concurrency getConcurrency() {
	return concurrency;
    }

    public Database getDatabase() {
	return database;
    }

    @Override
    public void close() {
	if(!isClosed()) {
            Exceptions.execute(() -> getResultSet().close());
        }
    }

    public Type getType() {
	return type;
    }

    public Layout getLayout() {
	init();
	return layout;
    }

    public int getCount() {
	if(count == -1) {
	    int pos = getPosition();
	    moveToEnd();
	    count = getPosition();
	    setPosition(pos);
	}

	return count;
    }

    public State getState() {
	return state;
    }

    private void checkModifySupport() {
	if(getConcurrency() == Concurrency.READ_ONLY) {
	    throw new IllegalStateException("Cursor concurrency is 'read only'.");
	}
    }

    public boolean isClosed() {
	return Exceptions.execute(getResultSet()::isClosed);
    }

    private void checkClosed() {
	if(isClosed()) {
	    throw new IllegalStateException("Cursor alredy closed.");
	}
    }

    /*private void checkRemoved() {	
     if (getState() == State.REMOVED) {
     throw new IllegalStateException(
     "Row on current position alredy removed.");
     }
     }*/
    
    private void checkForGet(Column c) {
	checkClosed();
	//checkRemoved();
	if(isBegin()) {
	    throw new IllegalStateException("Cursor on the begin position.");
	}
	
	if(isEnd()) {
	    throw new IllegalStateException("Cursor on the end position.");
	}

	Objects.requireNonNull(c, "Column can`t be null.");
	if(!getLayout().contains(c)) {
	    throw new IllegalArgumentException(
		    "Column isn`t contains in layout.");
	}
    }

    public void set(String columnName, Object value) {
	set(getLayout().getWithNameOrThrow(columnName), value);
    }

    public void set(int columnIndex, Object value) {
	set(getLayout().get(columnIndex), value);
    }

    public void set(Column c, Object object) {
	checkForGet(c);
	checkModifySupport();

	if(c.isReadOnly()) {
	    throw new IllegalArgumentException("Column is read only.");
	}

	Object v = object instanceof Value
		? ((Value) object).to(c.getType())
		: Value.to(c.getType(), object);

	if(!c.isNullable() && v == null) {
	    throw new IllegalArgumentException(
		    "Value is null, but column is not nullable.");
	}

	Exceptions.execute(() -> getResultSet().updateObject(c.getName(), v));
	state = State.UPDATING;
    }

    public Value get(int index) {
	return get(getLayout().get(index));
    }

    public Value get(String name) {
	return get(getLayout().getWithNameOrThrow(name));
    }

    public Value get(Column column) {
	checkForGet(column);

	return value.set(Exceptions.execute(
                () -> resultSet.getObject(column.getName())), column.getType());
    }

    /*public void remove() {
     checkClosed();
     checkRemoved();
	
     Exceptions.execute(getResultSet()::deleteRow);
     state = State.REMOVED;
     }

     public boolean isRemoved() {
     checkClosed();
     return getState() == State.REMOVED;
     }*/
    public boolean isUpdated() {
	checkClosed();
	return getState() == State.UPDATED;
    }

    public void update() {
	checkClosed();
	//checkRemoved();

	if(state == State.UPDATING && !isBegin() && !isEnd()) {
	    Exceptions.execute(getResultSet()::updateRow);
	    state = State.UPDATED;
	}
    }

    public abstract int getPosition();
    public abstract boolean isFirst();
    public abstract boolean isLast();
    public abstract boolean isBegin();
    public abstract boolean isEnd();
    public abstract boolean moveToFirst();
    public abstract boolean moveToBegin();
    public abstract boolean moveToEnd();
    public abstract boolean moveToLast();
    public abstract boolean moveToNext();
    public abstract boolean moveToPrevious();
    public abstract boolean setPosition(int position);
    public abstract boolean moveRelative(int offset);

    @Override
    public Iterator<Row> iterator() {
	if(getType() == Type.FORWARD_ONLY && !isBegin()) {
	    throw new IllegalStateException(
		    "Cursor is FORWARD_ONLY and not on the begin position.");
	}

	return new CursorIterator();
    }

    private class CursorIterator implements Iterator<Row> {

	private final Row row = new Row(getLayout());

	private int result = -1;
	private boolean readed = false;

	public CursorIterator() {
	    moveToBegin();
	}

	@Override
	public boolean hasNext() {
	    if(result == -1) {
		result = moveToNext() ? 1 : 0;
		readed = result == 0;
	    }
	    return result == 1;
	}

	@Override
	public Row next() {
	    if(!hasNext()) {
		throw new NoSuchElementException();
	    }
	    if(!readed) {
		row.reset();
		for(Column c : getLayout()) {
		    row.get(c).set(get(c).get(), c.getType());
		}
		readed = true;
	    }

	    result = -1;
	    return row;
	}

    }

    private static class ScrollableCursor extends Cursor {

	public ScrollableCursor(Database database, String sql, 
				Type type, Concurrency concurrency) {
	    super(database, sql, requireNonForward(type), concurrency);
	}
	
	private static Type requireNonForward(Type type) {
	    if(type == Type.FORWARD_ONLY) {
		throw new IllegalArgumentException(
			"Srollable cursor can`t be FORWARD_ONLY. Use ForwardCursor.");
	    }
	    
	    return type;
	}

	@Override
	public int getCount() {
	    if(count == -1) {
		Exceptions.execute(() -> {
		    ResultSet rs = getResultSet();
		    int pos = rs.isAfterLast() ? -1 : rs.getRow();
		    count = rs.last() ? rs.getRow() : 0;
		    if(pos == -1) rs.afterLast();
		    else rs.absolute(pos);
		});
	    }

	    return count;
	}

	@Override
	public int getPosition() {
	    return isEnd() ? getCount() : 
		    Exceptions.execute(getResultSet()::getRow) - 1;
	}

	@Override
	public boolean isFirst() {
	    return Exceptions.execute(getResultSet()::isFirst);
	}

	@Override
	public boolean isLast() {
	    return Exceptions.execute(getResultSet()::isLast);
	}

	@Override
	public boolean isBegin() {
	    return Exceptions.execute(getResultSet()::isBeforeFirst);
	}

	@Override
	public boolean isEnd() {
	    return Exceptions.execute(getResultSet()::isAfterLast);
	}

	@Override
	public boolean moveToFirst() {
	    update();
	    return Exceptions.execute(getResultSet()::first);
	}

	@Override
	public boolean moveToBegin() {
	    Exceptions.execute(getResultSet()::beforeFirst);
	    return true;
	}

	@Override
	public boolean moveToEnd() {
	    update();
	    Exceptions.execute(getResultSet()::afterLast);
	    return true;
	}

	@Override
	public boolean moveToLast() {
	    update();
	    return Exceptions.execute(getResultSet()::last);
	}

	@Override
	public boolean moveToNext() {
	    update();
	    return Exceptions.execute(getResultSet()::next);
	}
	
	@Override
	public boolean moveToPrevious() {
	    update();
	    return Exceptions.execute(getResultSet()::previous);
	}

	@Override
	public boolean setPosition(final int position) {
	    update();
	    return position == getCount() ? moveToEnd() :
		    Exceptions.execute(() -> getResultSet().absolute(position + 1));
	}

	@Override
	public boolean moveRelative(final int offset) {
	    update();
	    return setPosition(getPosition() + offset);
	}

	@Override
	public Iterator<Row> iterator() {
	    return new CursorIterator();
	}
    }

    private static class ForwardCursor extends Cursor {
	
	private int position = BEGIN_POSITION;

	public ForwardCursor(Database database, String sql, Concurrency concurrency) {
	    super(database, sql, Type.FORWARD_ONLY, concurrency);
	}

	@Override
	public int getPosition() {
	    return position;
	}

	@Override
	public boolean isFirst() {
	    return position == 0;
	}

	@Override
	public boolean isLast() {
	    return position == count - 1;
	}

	@Override
	public boolean isBegin() {
	    return position == BEGIN_POSITION;
	}

	@Override
	public boolean isEnd() {
	    return position == count && count != UNKNOWN_COUNT;
	}
	
	private static IllegalStateException cantBeToBack() {
	    throw new IllegalStateException(
		    "Cursor can`t be move to back. Is FORWARD_ONLY.");
	}

	@Override
	public boolean moveToFirst() {
	    if(position > 0) throw cantBeToBack();
	    if(position == BEGIN_POSITION) moveToNext();
	    return true;
	}

	@Override
	public boolean moveToBegin() {
	    if(position != BEGIN_POSITION) throw cantBeToBack();
	    update();
	    return true;
	}

	@Override
	public boolean moveToEnd() {
	    update();
	    while(moveToNext()) {}
	    return true;
	}

	@Override
	public int getCount() {
	    return count;
	}

	@Override
	public boolean moveToLast() {
	    throw new UnsupportedOperationException("moveToLast");
	}

	@Override
	public boolean moveToNext() {
	    update();
	    
	    if(Exceptions.execute(getResultSet()::next)) {
		position++;
		return true;
	    } else {
		if(count == UNKNOWN_COUNT) {
		    position++;
		    count = position;
		}
		return false;
	    }
	}

	@Override
	public boolean moveToPrevious() {
	    throw new UnsupportedOperationException("moveToPrevious");
	}

	@Override
	public boolean setPosition(int position) {
	    if(position < this.position) throw cantBeToBack();
	    return moveRelative(position - this.position);
	}

	@Override
	public boolean moveRelative(int offset) {
	    if(offset < 0) throw cantBeToBack();
	    update();
	    
	    while(offset > 0 && moveToNext()) {
		offset--;
	    }
	    
	    return offset == 0;
	}
    }
    
    public List<Map<Column, Object>> getData() {
	return getData(Column.class);
    }
    
    public <T> List<Map<T, Object>> getData(Class<T> c) {
	List<Map<T, Object>> data = new ArrayList<>();
	for(Row row : this) {
	    data.add(row.getData(c));
	}
	return data;
    }

}
