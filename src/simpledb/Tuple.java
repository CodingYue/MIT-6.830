package simpledb;


import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;


/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {

    private TupleDesc tupleDescription;
    private RecordId recordId;
    private Field fieldValues[];

    public static Tuple combine(Tuple t1, Tuple t2) {
        Tuple tuple = new Tuple(TupleDesc.combine(t1.getTupleDesc(), t2.getTupleDesc()));
        tuple.recordId = null;
        System.arraycopy(t1.fieldValues, 0, tuple.fieldValues, 0, t1.getTupleDesc().numFields());
        System.arraycopy(t2.fieldValues, 0, tuple.fieldValues, t1.getTupleDesc().numFields(),
                t2.getTupleDesc().numFields());
        return tuple;
    }

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDescription = td;
        this.fieldValues = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDescription;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        fieldValues[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldValues[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String result = "";
        for (int i = 0; i < tupleDescription.numFields(); ++i) {
            if (i > 0) {
                result += "\t";
            }
            result += getField(i).toString();
        }
        result += "\n";
        return result;
    }

    public boolean equals(Object o) {
        if (!Objects.equals(this.getClass(), o.getClass())) {
            return false;
        }
        Tuple that = (Tuple) o;
        return Objects.equals(recordId, that.recordId) && Objects.equals(tupleDescription, that.tupleDescription)
                && Arrays.equals(fieldValues, that.fieldValues);
    }
}
