package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

    private Type[] fieldTypes;
    private String[] fieldNames;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        TupleDesc tupleDesc = new TupleDesc();

        tupleDesc.fieldTypes = new Type[td1.numFields()+td2.numFields()];
        System.arraycopy(td1.fieldTypes, 0, tupleDesc.fieldTypes, 0, td1.numFields());
        System.arraycopy(td2.fieldTypes, 0, tupleDesc.fieldTypes, td1.numFields(), td2.numFields());

        tupleDesc.fieldNames = new String[tupleDesc.numFields()];
        System.arraycopy(td1.fieldNames, 0, tupleDesc.fieldNames, 0, td1.numFields());
        System.arraycopy(td2.fieldNames, 0, tupleDesc.fieldNames, td1.numFields(), td2.numFields());
        return tupleDesc;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.fieldTypes = new Type[typeAr.length];
        this.fieldNames = new String[typeAr.length];
        for (int i = 0; i < typeAr.length; ++i) {
            this.fieldTypes[i] = typeAr[i];
            if (fieldAr != null) {
                this.fieldNames[i] = fieldAr[i];
            }
        }
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, null);
    }

    public TupleDesc() {

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fieldTypes.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException();
        }
        return fieldNames[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); ++i) {
            if (Objects.equals(name, fieldNames[i])) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException();
        }
        return this.fieldTypes[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int i = 0; i < numFields(); ++i) {
            size += fieldTypes[i].getLen();
        }
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        TupleDesc that = (TupleDesc) o;
        if (this.numFields() != that.numFields()) {
            return false;
        }
        for (int i = 0; i < numFields(); ++i) {
            if (!Objects.equals(this.fieldTypes[i], that.fieldTypes[i])) return false;
            if (!Objects.equals(this.fieldNames[i], that.fieldNames[i])) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
