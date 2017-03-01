package simpledb;

import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {

    private PageId pageId;
    private int tupleNumber;

    /** Creates a new RecordId refering to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pageId = pid;
        this.tupleNumber = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if (this.getClass() != o.getClass()) {
            return false;
        }
        RecordId that = (RecordId) o;
        return Objects.equals(this.pageId, that.pageId) && this.tupleNumber == that.tupleNumber;
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	// some code goes here
    	throw new UnsupportedOperationException("implement this");
    	
    }
    
}
