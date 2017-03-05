package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {

    private final TransactionId transactionId;
    private final DbIterator child;
    private final int tableId;
    private final TupleDesc tupleDesc;

    private boolean hasBeenCalled;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.transactionId = t;
        this.child = child;
        this.tableId = tableid;
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        this.hasBeenCalled = false;
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (hasBeenCalled) {
            return null;
        }
        int insertCount = 0;
        hasBeenCalled = true;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            insertCount++;
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, tuple);
            } catch (IOException e) {
                throw new DbException("Insert failed");
            }
        }
        Tuple tuple = new Tuple(tupleDesc);
        tuple.setField(0, new IntField(insertCount));
        return tuple;
    }
}
