package simpledb;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private final TransactionId transactionId;
    private final int tableId;
    private final String tableAlias;

    private DbFileIterator tableIterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param transactionId The transaction this scan is running as a part of.
     * @param tableId the table to scan.
     * @param tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
     *         (note: this class is not responsible for handling a case where tableAlias
     *         or fieldName are null.  It shouldn't crash if they are, but the resulting
     *         name can be null.fieldName, tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId transactionId, int tableId, String tableAlias) {
        this.transactionId = transactionId;
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        tableIterator = Database.getCatalog().getDbFile(tableId).iterator(transactionId);
    }

    public void open()
        throws DbException, TransactionAbortedException {
        tableIterator.open();
    }

    public String getAlias() {
        return tableAlias;
    }

    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupleDesc = Database.getCatalog().getDbFile(tableId).getTupleDesc();
        Type[] fieldTypes = new Type[tupleDesc.numFields()];
        String[] fieldNames = new String[tupleDesc.numFields()];
        for (int i = 0; i < tupleDesc.numFields(); ++i) {
            fieldNames[i] = tableAlias + tupleDesc.getFieldName(i);
            fieldTypes[i] = tupleDesc.getFieldType(i);
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return tableIterator != null && tableIterator.hasNext();
    }

    public Tuple next()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        if (tableIterator == null) {
            throw new NoSuchElementException();
        }
        return tableIterator.next();
    }

    public void close() {
        tableIterator.close();
    }

    public void rewind()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        open();
    }
}
