package dev.zpq.spark;

import org.apache.arrow.vector.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.*;
import org.apache.spark.unsafe.types.UTF8String;
import java.math.BigDecimal;

/** Spark widens Parquet unsigned integers; Arrow faithfully retains their unsigned physical type. */
final class ZpqArrowColumnVector extends ColumnVector {
    private final FieldVector vector;
    private final ArrowColumnVector base;
    private final boolean unsigned;

    ZpqArrowColumnVector(FieldVector vector, DataType expected) {
        super(expected);
        this.vector = vector;
        unsigned = vector instanceof UInt1Vector || vector instanceof UInt2Vector
            || vector instanceof UInt4Vector || vector instanceof UInt8Vector;
        base = unsigned ? null : new ArrowColumnVector(vector);
    }
    // Storage belongs to ArrowReader and lasts until the next pull or reader close.
    @Override public void close() {}
    @Override public boolean hasNull() { return vector.getNullCount() != 0; }
    @Override public int numNulls() { return vector.getNullCount(); }
    @Override public boolean isNullAt(int row) { return vector.isNull(row); }
    @Override public boolean getBoolean(int row) { return base.getBoolean(row); }
    private long unsignedLong(int row) {
        if (vector instanceof UInt1Vector v) return v.get(row) & 0xff;
        if (vector instanceof UInt2Vector v) return v.get(row);
        if (vector instanceof UInt4Vector v) return Integer.toUnsignedLong(v.get(row));
        return ((UInt8Vector) vector).get(row);
    }
    @Override public byte getByte(int row) { return unsigned ? (byte) unsignedLong(row) : base.getByte(row); }
    @Override public short getShort(int row) { return unsigned ? (short) unsignedLong(row) : base.getShort(row); }
    @Override public int getInt(int row) { return unsigned ? (int) unsignedLong(row) : base.getInt(row); }
    @Override public long getLong(int row) { return unsigned ? unsignedLong(row) : base.getLong(row); }
    @Override public float getFloat(int row) { return base.getFloat(row); }
    @Override public double getDouble(int row) { return base.getDouble(row); }
    @Override public Decimal getDecimal(int row, int precision, int scale) {
        if (unsigned) return Decimal.apply(new BigDecimal(Long.toUnsignedString(unsignedLong(row))), precision, scale);
        return base.getDecimal(row, precision, scale);
    }
    @Override public UTF8String getUTF8String(int row) {
        if (vector instanceof VarBinaryVector binary) {
            int start = binary.getStartOffset(row);
            return UTF8String.fromAddress(null, binary.getDataBuffer().memoryAddress() + start,
                binary.getEndOffset(row) - start);
        }
        return base.getUTF8String(row);
    }
    @Override public byte[] getBinary(int row) { return base.getBinary(row); }
    @Override public ColumnarArray getArray(int row) { throw new UnsupportedOperationException("nested Arrow result"); }
    @Override public ColumnarMap getMap(int row) { throw new UnsupportedOperationException("nested Arrow result"); }
    @Override public ColumnVector getChild(int ordinal) { throw new UnsupportedOperationException("nested Arrow result"); }
}
