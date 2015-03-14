/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ndemyanovskyi.derby;

import com.ndemyanovskyi.util.DateTimeFormatters;
import com.ndemyanovskyi.util.Unmodifiable;
import com.ndemyanovskyi.throwable.Exceptions;
import com.ndemyanovskyi.throwable.ValueConvertException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import javax.sql.rowset.serial.SerialBlob;

public class Value {

    private Type<?> type;
    private Object value;

    private Map<Class<?>, Object> convertValues;
    
    protected Value() {}
    
    protected Value(Object value) {
	set(value);
    }
    
    protected Value(Object value, Type<?> type) {
	set(value, type);
    }
    
    Value set(Object value, Type<?> type) {
        if(value instanceof java.util.Date) value = LocalDate.parse(value.toString());
	if(value instanceof java.sql.Time) value = LocalTime.parse(value.toString());
	if(value instanceof java.sql.Timestamp) value = LocalDateTime.parse(value.toString());
	if(!Objects.equals(this.value, value)) {
	    this.type = value != null ? Type.from(value) : type;
	    this.value = value != null ? to(type, value) : null;
	    
	    if(convertValues != null) {
		convertValues.clear();
	    }
	}
	return this;
    }

    Value set(Object value) {
	return set(value, Type.from(value));
    }

    public Type<?> getType() {
	return type;
    }

    private <T> T getConvertValue(Class<T> c) {
	if(convertValues == null) {
	    convertValues = new WeakHashMap<>();
	}

	return (T) convertValues.get(c);
    }

    private <T> void setConvertValue(Class<T> c, T value) {
	if(convertValues == null) {
	    convertValues = new WeakHashMap<>();
	}

	convertValues.put(c, value);
    }
    
    public boolean canTo(Type<?> type) {
	return canTo(type.getRepresentClass());
    }
    
    public boolean canTo(Class<?> c) {
	try {
	    to(c);
	    return true;
	} catch(IllegalArgumentException ex) {
	    return false;
	}
    }

    public <T> T to(Type<T> type) {
	return to(type.getRepresentClass());
    }

    public <T> T to(Class<T> c) {
	return to(c, value);
    }

    @Override
    public String toString() {
	if(isImmutable(value)) {
	    String res = getConvertValue(String.class);
	    if(res == null) {
		res = toString(value);
		if(res == null) res = "null";
		setConvertValue(String.class, res);
	    }
	    return res;
	}
	
	return toString(value);
    }
    
    public LocalTime toLocalTime() {
	if(isImmutable(value)) {
	    LocalTime res = getConvertValue(LocalTime.class);
	    if(res == null) {
		res = toLocalTime(value);
		setConvertValue(LocalTime.class, res);
	    }
	    return res;
	}
	return toLocalTime(value);
    }
    
    public LocalDateTime toLocalDateTime() {
	if(isImmutable(value)) {
	    LocalDateTime res = getConvertValue(LocalDateTime.class);
	    if(res == null) {
		res = toLocalDateTime(value);
		setConvertValue(LocalDateTime.class, res);
	    }
	    return res;
	}
	
	return toLocalDateTime(value);
    }
    
    public LocalDate toLocalDate() {
	if(isImmutable(value)) {
	    LocalDate res = getConvertValue(LocalDate.class);
	    if(res == null) {
		res = toLocalDate(value);
		setConvertValue(LocalDate.class, res);
	    }
	    return res;
	}
	
	return toLocalDate(value);
    }

    public Blob toBlob() {
	return toBlob(value);
    }

    public Short toShort() {
	if(isImmutable(value)) {
	    Short res = getConvertValue(Short.class);
	    if(res == null) {
		res = toShort(value);
		setConvertValue(Short.class, res);
	    }
	    return res;
	}
	
	return toShort(value);
    }

    public Integer toInteger() {
	if(isImmutable(value)) {
	    Integer res = getConvertValue(Integer.class);
	    if(res == null) {
		res = toInteger(value);
		setConvertValue(Integer.class, res);
	    }
	    return res;
	}
	
	return toInteger(value);
    }

    public Long toLong() {
	if(isImmutable(value)) {
	    Long res = getConvertValue(Long.class);
	    if(res == null) {
		res = toLong(value);
		setConvertValue(Long.class, res);
	    }
	    return res;
	}
	
	return toLong(value);
    }

    public Float toFloat() {
	if(isImmutable(value)) {
	    Float res = getConvertValue(Float.class);
	    if(res == null) {
		res = toFloat(value);
		setConvertValue(Float.class, res);
	    }
	    return res;
	}
	
	return toFloat(value);
    }

    public Double toDouble() {
	if(isImmutable(value)) {
	    Double res = getConvertValue(Double.class);
	    if(res == null) {
		res = toDouble(value);
		setConvertValue(Double.class, res);
	    }
	    return res;
	}
	
	return toDouble(value);
    }

    public BigDecimal toBigDecimal() {
	if(isImmutable(value)) {
	    BigDecimal res = getConvertValue(BigDecimal.class);
	    if(res == null) {
		res = toBigDecimal(value);
		setConvertValue(BigDecimal.class, res);
	    }
	    return res;
	}
	
	return toBigDecimal(value);
    }

    public Object get() {
	return value;
    }
    
    public static boolean isImmutable(Class<?> c) {
	Objects.requireNonNull(c);
	
	if(String.class.isAssignableFrom(c)) return true;
	if(LocalDate.class.isAssignableFrom(c)) return true;
	if(LocalTime.class.isAssignableFrom(c)) return true;
	if(LocalDateTime.class.isAssignableFrom(c)) return true;
	if(Number.class.isAssignableFrom(c)) return true;
	if(Blob.class.isAssignableFrom(c)) return false;
	
	throw new IllegalArgumentException("Class not supported.");
    }
    
    public static boolean isImmutable(Object value) {
	return value != null && isImmutable(value.getClass());
    }
    
    
    //<editor-fold defaultstate="collapsed" desc="'is' methods">
    public boolean is(Class<?> c) {
	return c.isInstance(value);
    }
    public boolean isString() {
	return String.class.isInstance(value);
    }
    
    public boolean isShort() {
	return Short.class.isInstance(value);
    }
    
    public boolean isInteger() {
	return Integer.class.isInstance(value);
    }
    
    public boolean isLong() {
	return Long.class.isInstance(value);
    }
    
    public boolean isFloat() {
	return Float.class.isInstance(value);
    }
    
    public boolean isDouble() {
	return Double.class.isInstance(value);
    }
    
    public boolean isBigDecimal() {
	return BigDecimal.class.isInstance(value);
    }
    
    public boolean isLocalTime() {
	return LocalTime.class.isInstance(value);
    }
    
    public boolean isLocalDateTime() {
	return LocalDateTime.class.isInstance(value);
    }
    
    public boolean isLocalDate() {
	return LocalDate.class.isInstance(value);
    }
    
    public boolean isBlob() {
	return Blob.class.isInstance(value);
    }
    
    public boolean isNull() {
	return value == null;
    }
    
    public boolean nonNull() {
	return value != null;
    }
    
    public boolean isImmutable() {
	return !isNull() && isImmutable(value.getClass());
    }
    
    public boolean isMutable() {
	return !isNull() && !isImmutable(value.getClass());
    }
    //</editor-fold>
    
    //<editor-fold defaultstate="collapsed" desc="Convert methods">
   
    public static <T> T to(Type<T> t, Object value) {
	return to(t.getRepresentClass(), value);
    }

    @SuppressWarnings("unchecked")
    public static <T> T to(Class<T> c, Object value) {
	Objects.requireNonNull(c);
	
	if(LocalDate.class.isAssignableFrom(c))	    return (T) toLocalDate(value);
	if(LocalTime.class.isAssignableFrom(c))	    return (T) toLocalTime(value);
	if(Blob.class.isAssignableFrom(c))	    return (T) toBlob(value);
	if(Long.class.isAssignableFrom(c))	    return (T) toLong(value);
	if(Float.class.isAssignableFrom(c))	    return (T) toFloat(value);
	if(Short.class.isAssignableFrom(c))	    return (T) toShort(value);
	if(Double.class.isAssignableFrom(c))	    return (T) toDouble(value);
	if(String.class.isAssignableFrom(c))	    return (T) toString(value);
	if(Integer.class.isAssignableFrom(c))	    return (T) toInteger(value);
	if(LocalDateTime.class.isAssignableFrom(c))	    return (T) toLocalDateTime(value);
	if(BigDecimal.class.isAssignableFrom(c))    return (T) toBigDecimal(value);

	if(value == null) return null;
	
	throw new ValueConvertException(value, c);
    }
    
    public static String toString(Object value) {
	if(value == null) return "null";
	Class<?> c = value.getClass();
	return c.isAssignableFrom(Blob.class)
                ? toString(toObject((Blob) value))
                : value.toString();
    }
    
    public static LocalTime toLocalTime(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (LocalTime.class.isAssignableFrom(c)) return (LocalTime) value;
	if (String.class.isAssignableFrom(c)) return LocalTime.parse((String) value);
	if (LocalDateTime.class.isAssignableFrom(c)) return ((LocalDateTime) value).toLocalTime();
	//if (Number.class.isAssignableFrom(c)) return LocalTime.parse((int) (((Number) value).longValue() % LocalTime.SECONDS_PER_DAY));
	if (Blob.class.isAssignableFrom(c)) return toLocalTime(toObject((Blob) value));
	
	throw new ValueConvertException(value, LocalTime.class);
    }
    
    public static LocalDateTime toLocalDateTime(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (LocalDateTime.class.isAssignableFrom(c)) return (LocalDateTime) value;
	if (String.class.isAssignableFrom(c)) return LocalDateTime.parse((String) value);
	if (LocalTime.class.isAssignableFrom(c)) return LocalDateTime.of(LocalDate.MIN, (LocalTime) value);
	//if (Number.class.isAssignableFrom(c)) return LocalDateTime.parse(((Number) value).longValue());
	if (Blob.class.isAssignableFrom(c)) return toLocalDateTime(toObject((Blob) value));
	
	throw new ValueConvertException(value, LocalDateTime.class);
    }
    
    private static final Set<String> DATE_LAYOUTS = 
            Unmodifiable.set("dd.MM.yyyy", "dd-MM-yyyy", "yyyy-MM-dd", "yyyyMMdd");
    
    public static LocalDate toLocalDate(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	if (LocalDate.class.isAssignableFrom(c)) return (LocalDate) value;
	if (CharSequence.class.isAssignableFrom(c)) {
            for (String layout : DATE_LAYOUTS) {
                try {
                    return LocalDate.parse((CharSequence) value, DateTimeFormatters.of(layout));
                } catch(DateTimeParseException ex) {}
            }
        }
	if (LocalDateTime.class.isAssignableFrom(c)) return ((LocalDateTime) value).toLocalDate();
	if (Blob.class.isAssignableFrom(c)) return toLocalDate(toObject((Blob) value));
	
	throw new ValueConvertException(value, LocalDate.class);
    }
    
    public static Short toShort(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if(Short.class.isAssignableFrom(c))    return (Short) value;
	if(String.class.isAssignableFrom(c))   return Short.valueOf((String) value);
	if(Number.class.isAssignableFrom(c))   return ((Number) value).shortValue();
	if(Blob.class.isAssignableFrom(c))     return toShort(toObject((Blob) value));
	
	throw new ValueConvertException(value, Short.class);
    }
    
    public static Integer toInteger(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
        try {
            if(Integer.class.isAssignableFrom(c)) return (Integer) value;
            if(String.class.isAssignableFrom(c)) return Integer.valueOf((String) value);
            if(Number.class.isAssignableFrom(c)) return ((Number) value).intValue();
            if(Blob.class.isAssignableFrom(c)) return toInteger(toObject((Blob) value));
        } catch(IllegalArgumentException ex) {
            throw new ValueConvertException(value, Integer.class, ex);
        } 
	
	throw new ValueConvertException(value, Integer.class);
    }
    
    public static Long toLong(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (Long.class.isAssignableFrom(c)) return (Long) value;
	if (String.class.isAssignableFrom(c)) return Long.valueOf((String) value);
	//if (LocalDate.class.isAssignableFrom(c)) return (long) ((LocalDate) value).inDays();
	//if (LocalTime.class.isAssignableFrom(c)) return (long) ((LocalTime) value).inSeconds();
	//if (LocalDateTime.class.isAssignableFrom(c)) return ((LocalDateTime) value).inNanos().longValue();
	if (Number.class.isAssignableFrom(c)) return ((Number) value).longValue();
	if (Blob.class.isAssignableFrom(c)) return toLong(toObject((Blob) value));
	
	throw new ValueConvertException(value, Long.class);
    }
    
    public static Float toFloat(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (Float.class.isAssignableFrom(c)) return (Float) value;
	if (String.class.isAssignableFrom(c)) return Float.valueOf((String) value);
	//if (LocalDate.class.isAssignableFrom(c)) return (float) ((LocalDate) value).inDays();
	//if (LocalTime.class.isAssignableFrom(c)) return (float) ((LocalTime) value).inSeconds();
	//if (LocalDateTime.class.isAssignableFrom(c)) return ((LocalDateTime) value).inNanos().floatValue();
	if (Number.class.isAssignableFrom(c)) return ((Number) value).floatValue();
	if (Blob.class.isAssignableFrom(c)) return toFloat(toObject((Blob) value));
	
	throw new ValueConvertException(value, Float.class);
    }
    
    public static Double toDouble(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (Double.class.isAssignableFrom(c)) return (Double) value;
	if (String.class.isAssignableFrom(c)) return Double.valueOf((String) value);
	//if (LocalDate.class.isAssignableFrom(c)) return (double) ((LocalDate) value).inDays();
	//if (LocalTime.class.isAssignableFrom(c)) return (double) ((LocalTime) value).inSeconds();
	//if (LocalDateTime.class.isAssignableFrom(c)) return ((LocalDateTime) value).inNanos().doubleValue();
	if (Number.class.isAssignableFrom(c)) return ((Number) value).doubleValue();
	if (Blob.class.isAssignableFrom(c)) return toDouble(toObject((Blob) value));
	
	throw new ValueConvertException(value, Double.class);
    }
    
    public static BigDecimal toBigDecimal(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (BigDecimal.class.isAssignableFrom(c)) return (BigDecimal) value;
	if (String.class.isAssignableFrom(c)) return new BigDecimal((String) value);
	//if (LocalDate.class.isAssignableFrom(c)) return BigDecimal.valueOf(((LocalDate) value).inDays());
	//if (LocalTime.class.isAssignableFrom(c)) return BigDecimal.valueOf(((LocalTime) value).inSeconds());
	//if (LocalDateTime.class.isAssignableFrom(c)) return new BigDecimal(((LocalDateTime) value).inNanos());
	if (Number.class.isAssignableFrom(c)) return BigDecimal.valueOf(((Number) value).doubleValue());
	if (Blob.class.isAssignableFrom(c)) return toBigDecimal(toObject((Blob) value));
	
	throw new ValueConvertException(value, BigDecimal.class);
    }
    
    public static Object toObject(Blob value) {
	if(value == null) return null;
	
        return Exceptions.execute(() -> {
            try {
                return new ObjectInputStream(value.getBinaryStream()).readObject();
            }catch(IOException | ClassNotFoundException ex) {
                throw new ValueConvertException(Blob.class, Object.class, ex);
            }
        });
    }
    
    public static Blob toBlob(Object value) {
	if(value == null) return null;
	
	Class<?> c = value.getClass();
	
	if (Blob.class.isAssignableFrom(c)) return (Blob) value;
	
	return Exceptions.execute(() -> {
	    try {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(value);
		oos.flush(); baos.flush();
		byte[] array = baos.toByteArray();
		oos.close(); baos.close();
		return new SerialBlob(array);
	    } catch(IOException ex) {
		throw new ValueConvertException(value, Blob.class, ex);
	    }
	});
    }
    //</editor-fold>

}
