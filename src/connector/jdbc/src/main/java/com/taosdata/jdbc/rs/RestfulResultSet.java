package com.taosdata.jdbc.rs;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.taosdata.jdbc.*;
import com.taosdata.jdbc.utils.UtcTimestampUtil;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;

public class RestfulResultSet extends AbstractResultSet implements ResultSet {
    private volatile boolean isClosed;
    private int pos = -1;


    private final String database;
    private final Statement statement;
    // data
    private final ArrayList<ArrayList<Object>> resultSet;
    // meta
    private ArrayList<String> columnNames;
    private ArrayList<Field> columns;
    private RestfulResultSetMetaData metaData;

    /**
     * 由一个result的Json构造结果集，对应执行show databases, show tables等这些语句，返回结果集，但无法获取结果集对应的meta，统一当成String处理
     *
     * @param resultJson: 包含data信息的结果集，有sql返回的结果集
     ***/
    public RestfulResultSet(String database, Statement statement, JSONObject resultJson) throws SQLException {
        this.database = database;
        this.statement = statement;

        // column metadata
        JSONArray columnMeta = resultJson.getJSONArray("column_meta");
        columnNames = new ArrayList<>();
        columns = new ArrayList<>();
        for (int colIndex = 0; colIndex < columnMeta.size(); colIndex++) {
            JSONArray col = columnMeta.getJSONArray(colIndex);
            String col_name = col.getString(0);
            int taos_type = col.getInteger(1);
            int col_type = TSDBConstants.taosType2JdbcType(taos_type);
            int col_length = col.getInteger(2);
            columnNames.add(col_name);
            columns.add(new Field(col_name, col_type, col_length, "", taos_type));
        }
        this.metaData = new RestfulResultSetMetaData(this.database, columns, this);

        // row data
        JSONArray data = resultJson.getJSONArray("data");
        resultSet = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
            ArrayList row = new ArrayList();
            JSONArray jsonRow = data.getJSONArray(rowIndex);
            for (int colIndex = 0; colIndex < jsonRow.size(); colIndex++) {
                row.add(parseColumnData(jsonRow, colIndex, columns.get(colIndex).taos_type));
            }
            resultSet.add(row);
        }
    }

    private Object parseColumnData(JSONArray row, int colIndex, int taosType) throws SQLException {
        switch (taosType) {
            case TSDBConstants.TSDB_DATA_TYPE_BOOL:
                return row.getBoolean(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TINYINT:
                return row.getByte(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_SMALLINT:
                return row.getShort(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_INT:
                return row.getInteger(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_BIGINT:
                return row.getLong(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_FLOAT:
                return row.getFloat(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_DOUBLE:
                return row.getDouble(colIndex);
            case TSDBConstants.TSDB_DATA_TYPE_TIMESTAMP: {
                if (row.get(colIndex) == null)
                    return null;
                String timestampFormat = this.statement.getConnection().getClientInfo(TSDBDriver.PROPERTY_KEY_TIMESTAMP_FORMAT);
                if ("TIMESTAMP".equalsIgnoreCase(timestampFormat)) {
                    Long value = row.getLong(colIndex);
                    //TODO:
                    if (value < 1_0000_0000_0000_0L)
                        return new Timestamp(value);
                    long epochSec = value / 1000_000l;
                    long nanoAdjustment = value % 1000_000l * 1000l;
                    return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                }
                if ("UTC".equalsIgnoreCase(timestampFormat)) {
                    String value = row.getString(colIndex);
                    long epochSec = Timestamp.valueOf(value.substring(0, 19).replace("T", " ")).getTime() / 1000;
                    int fractionalSec = Integer.parseInt(value.substring(20, value.length() - 5));
                    long nanoAdjustment = 0;
                    if (value.length() > 28) {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSSSSS+0x00
                        nanoAdjustment = fractionalSec * 1000l;
                    } else {
                        // ms timestamp: yyyy-MM-ddTHH:mm:ss.SSS+0x00
                        nanoAdjustment = fractionalSec * 1000_000l;
                    }
                    ZoneOffset zoneOffset = ZoneOffset.of(value.substring(value.length() - 5));
                    Instant instant = Instant.ofEpochSecond(epochSec, nanoAdjustment).atOffset(zoneOffset).toInstant();
                    return Timestamp.from(instant);
                }
                String value = row.getString(colIndex);
                if (value.length() <= 23)    // ms timestamp: yyyy-MM-dd HH:mm:ss.SSS
                    return row.getTimestamp(colIndex);
                // us timestamp: yyyy-MM-dd HH:mm:ss.SSSSSS
                long epochSec = Timestamp.valueOf(value.substring(0, 19)).getTime() / 1000;
                long nanoAdjustment = Integer.parseInt(value.substring(20)) * 1000l;
                Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
                return timestamp;
            }
            case TSDBConstants.TSDB_DATA_TYPE_BINARY:
                return row.getString(colIndex) == null ? null : row.getString(colIndex).getBytes();
            case TSDBConstants.TSDB_DATA_TYPE_NCHAR:
                return row.getString(colIndex) == null ? null : row.getString(colIndex);
            default:
                return row.get(colIndex);
        }
    }

    public class Field {
        String name;
        int type;
        int length;
        String note;
        int taos_type;

        public Field(String name, int type, int length, String note, int taos_type) {
            this.name = name;
            this.type = type;
            this.length = length;
            this.note = note;
            this.taos_type = taos_type;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        pos++;
        if (pos <= resultSet.size() - 1) {
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        synchronized (RestfulResultSet.class) {
            this.isClosed = true;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return resultSet.isEmpty();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof byte[])
            return new String((byte[]) value);
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return false;
        if (value instanceof Boolean)
            return (boolean) value;
        return Boolean.valueOf(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Byte.MIN_VALUE)
            return 0;
        if (valueAsLong < Byte.MIN_VALUE || valueAsLong > Byte.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.TINYINT);

        return (byte) valueAsLong;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_NUMERIC_VALUE_OUT_OF_RANGE,
                "'" + valueAsString + "' in column '" + columnIndex + "' is outside valid range for the jdbcType " + TSDBConstants.jdbcType2TaosTypeName(jdbcType));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Short.MIN_VALUE)
            return 0;
        if (valueAsLong < Short.MIN_VALUE || valueAsLong > Short.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
        return (short) valueAsLong;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Integer.MIN_VALUE)
            return 0;
        if (valueAsLong < Integer.MIN_VALUE || valueAsLong > Integer.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.INTEGER);
        return (int) valueAsLong;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
        }

        long valueAsLong = 0;
        try {
            valueAsLong = Long.parseLong(value.toString());
            if (valueAsLong == Long.MIN_VALUE)
                return 0;
        } catch (NumberFormatException e) {
            throwRangeException(value.toString(), columnIndex, Types.BIGINT);
        }
        return valueAsLong;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        if (value instanceof Float || value instanceof Double)
            return (float) value;
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return 0;
        if (value instanceof Double || value instanceof Float)
            return (double) value;
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof byte[])
            return (byte[]) value;
        if (value instanceof String)
            return ((String) value).getBytes();
        if (value instanceof Long)
            return Longs.toByteArray((long) value);
        if (value instanceof Integer)
            return Ints.toByteArray((int) value);
        if (value instanceof Short)
            return Shorts.toByteArray((short) value);
        if (value instanceof Byte)
            return new byte[]{(byte) value};

        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        return Date.valueOf(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Time(((Timestamp) value).getTime());
        return Time.valueOf(value.toString());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return (Timestamp) value;
//        if (value instanceof Long) {
//            if (1_0000_0000_0000_0L > (long) value)
//                return Timestamp.from(Instant.ofEpochMilli((long) value));
//            long epochSec = (long) value / 1000_000L;
//            long nanoAdjustment = (long) ((long) value % 1000_000L * 1000);
//            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
//        }
        return Timestamp.valueOf(value.toString());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        return resultSet.get(pos).get(columnIndex - 1);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in resultSet");
        return columnIndex + 1;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, resultSet.get(pos).size());

        Object value = resultSet.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;

        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
            return new BigDecimal(Long.valueOf(value.toString()));
        if (value instanceof Double || value instanceof Float)
            return new BigDecimal(Double.valueOf(value.toString()));
        if (value instanceof Timestamp)
            return new BigDecimal(((Timestamp) value).getTime());
        return new BigDecimal(value.toString());
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.pos == -1 && this.resultSet.size() != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.pos >= resultSet.size() && this.resultSet.size() != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        return this.pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.resultSet.size() == 0)
            return false;
        return this.pos == (this.resultSet.size() - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        synchronized (this) {
            if (this.resultSet.size() > 0) {
                this.pos = this.resultSet.size();
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        if (this.resultSet.size() == 0)
            return false;

        synchronized (this) {
            this.pos = 0;
        }
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        if (this.resultSet.size() == 0)
            return false;
        synchronized (this) {
            this.pos = this.resultSet.size() - 1;
        }
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);
        int row;
        synchronized (this) {
            if (this.pos < 0 || this.pos >= this.resultSet.size())
                return 0;
            row = this.pos + 1;
        }
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

//        if (this.resultSet.size() == 0)
//            return false;
//
//        if (row == 0) {
//            beforeFirst();
//            return false;
//        } else if (row == 1) {
//            return first();
//        } else if (row == -1) {
//            return last();
//        } else if (row > this.resultSet.size()) {
//            afterLast();
//            return false;
//        } else {
//            if (row < 0) {
//                // adjust to reflect after end of result set
//                int newRowPosition = this.resultSet.size() + row + 1;
//                if (newRowPosition <= 0) {
//                    beforeFirst();
//                    return false;
//                } else {
//                    return absolute(newRowPosition);
//                }
//            } else {
//                row--; // adjust for index difference
//                this.pos = row;
//                return true;
//            }
//        }

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public boolean previous() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_UNSUPPORTED_METHOD);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Statement getStatement() throws SQLException {
        if (isClosed())
            throw TSDBError.createSQLException(TSDBErrorNumbers.ERROR_RESULTSET_CLOSED);

        return this.statement;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO：did not use the specified timezone in cal
        return getTimestamp(columnIndex);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }


}
