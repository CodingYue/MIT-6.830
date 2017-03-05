package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    private long pageCount;
    private final File file;
    private final TupleDesc tupleDescription;
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDescription = td;
        pageCount = f.length() / BufferPool.PAGE_SIZE;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDescription;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws NoSuchElementException {
        try {
            RandomAccessFile randomAccessFile;
            randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(BufferPool.PAGE_SIZE * pid.pageno());
            byte data[] = new byte[BufferPool.PAGE_SIZE];
            randomAccessFile.read(data);

            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            throw  new NoSuchElementException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public long numPages() {
        return pageCount;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        if (t.getRecordId() != null) {
            HeapPage page = (HeapPage) readPage(t.getRecordId().getPageId());
            page.insertTuple(t);
        } else {
            for (int i = 0; i < pageCount; ++i) {
                HeapPage page = (HeapPage) readPage(new HeapPageId())
            }
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
}

