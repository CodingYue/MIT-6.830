package simpledb;

import org.omg.CORBA.PUBLIC_MEMBER;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {

    private static class LockManager {
        private final Map<TransactionId, Set<PageId>> tidToPages;
        private final Map<PageId, Permissions> pageToPermissions;
        private final Map<PageId, Set<TransactionId>> pageToTids;
        private final Map<TransactionId, Set<TransactionId>> waitingForTids;
        private final Set<TransactionId> aliveTids;

        public LockManager() {
            tidToPages = new HashMap<TransactionId, Set<PageId>>();
            pageToPermissions = new HashMap<PageId, Permissions>();
            pageToTids = new HashMap<PageId, Set<TransactionId>>();
            waitingForTids = new HashMap<TransactionId, Set<TransactionId>>();
            aliveTids = new HashSet<TransactionId>();
        }

        private boolean checkLockStatus(TransactionId tid, PageId pid, Permissions perm) {
            if (!pageToPermissions.containsKey(pid)) {
                return true;
            }
            boolean multipleTransactions = pageToTids.containsKey(pid) && pageToTids.get(pid).size() >= 2;

            if (pageToPermissions.get(pid).equals(Permissions.READ_ONLY)) {
                if (perm.equals(Permissions.READ_ONLY)) {
                    return true;
                } else {
                    return !multipleTransactions && pageToTids.get(pid).contains(tid);
                }
            } else {
                return pageToTids.get(pid).contains(tid);
            }
        }

        private boolean acquireLock(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException{
            aliveTids.add(tid);
            while (aliveTids.contains(tid) && !checkLockStatus(tid, pid, perm));
            if (!aliveTids.contains(tid)) {
                return false;
            }
            if (!tidToPages.containsKey(tid)) {
                tidToPages.put(tid, new HashSet<PageId>());
            }
            if (!pageToTids.containsKey(pid)) {
                pageToTids.put(pid, new HashSet<TransactionId>());
            }

            pageToPermissions.put(pid, perm);
            pageToTids.get(pid).add(tid);
            tidToPages.get(tid).add(pid);
            return true;
        }

        private boolean releaseLock(TransactionId tid, PageId pid) {
            if (!tidToPages.containsKey(tid) || !tidToPages.get(tid).contains(pid)) {
                return false;
            }
            if (!pageToTids.containsKey(pid) || !pageToTids.get(pid).contains(tid)) {
                return false;
            }
            tidToPages.get(tid).remove(pid);
            if (tidToPages.get(tid).isEmpty()) {
                tidToPages.remove(tid);
            }
            pageToTids.get(pid).remove(tid);
            if (pageToTids.get(pid).isEmpty()) {
                pageToTids.remove(pid);
                pageToPermissions.remove(pid);
            }
            return true;
        }

        private void releaseTransaction(TransactionId tid) {
            Set<PageId> pids = new HashSet<PageId>();
            for (PageId pid : tidToPages.get(tid)) {
                pids.add(pid);
            }
            for (PageId pid : pids) {
                releaseLock(tid, pid);
            }
            waitingForTids.remove(tid);
            aliveTids.remove(tid);
        }

        private boolean isHoldsLock(TransactionId tid, PageId pid) {
            return pageToTids.containsKey(pid) && pageToTids.get(pid).contains(tid)
                    && tidToPages.containsKey(tid) && tidToPages.get(tid).contains(pid);
        }

        private Set<PageId> getPageHoldedByTransaction(TransactionId tid) {
            return tidToPages.get(tid);
        }

        private void detectDeadlocks(TransactionId originalTid, TransactionId currentTid)
                throws TransactionAbortedException{
            if (originalTid.equals(currentTid)) {
                throw new TransactionAbortedException();

            }
            if (waitingForTids.containsKey(currentTid)) {
                for (TransactionId tid : waitingForTids.get(currentTid)) {
                    detectDeadlocks(originalTid, tid);
                }
            }
        }

        private void detectDeadlocks(TransactionId originalTid, PageId pid)
                throws TransactionAbortedException {
            if (pageToTids.containsKey(pid)) {
                for (TransactionId tid : pageToTids.get(pid)) {
                    if (!tid.equals(originalTid)) {
                        detectDeadlocks(originalTid, tid);
                    }
                }
            }
            if (!waitingForTids.containsKey(originalTid)) {
                waitingForTids.put(originalTid, new HashSet<TransactionId>());
            }
            if (pageToTids.containsKey(pid)) {
                for (TransactionId tid : pageToTids.get(pid)) {
                    waitingForTids.get(tid).add(tid);
                }
            }
        }

    }

    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final LockManager lockManager;
    private final Page[] pagePool;
    private final Map<PageId, Integer> cachedPageIndex;
    private final Set<Integer> idlePageIdx;
    private final Map<PageId, Integer> latestUsedTimestamp;

    private int timestamp;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        pagePool = new Page[numPages];
        cachedPageIndex = new HashMap<PageId, Integer>();
        idlePageIdx = new HashSet<Integer>();
        for (int i = 0; i < numPages; ++i) {
            idlePageIdx.add(i);
        }
        latestUsedTimestamp = new HashMap<PageId, Integer>();
        timestamp = 0;
        lockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        lockManager.detectDeadlocks(tid, pid);
        lockManager.acquireLock(tid, pid, perm);

        timestamp++;
        latestUsedTimestamp.put(pid, timestamp);

        if (cachedPageIndex.containsKey(pid)) {
            return pagePool[cachedPageIndex.get(pid)];
        }

        if (idlePageIdx.size() == 0) {
            evictPage();
        }
        int idleIdx = idlePageIdx.iterator().next();
        cachedPageIndex.put(pid, idleIdx);
        idlePageIdx.remove(idleIdx);
        pagePool[idleIdx] = Database.getCatalog().getDbFile(
                pid.getTableId()).readPage(pid);
        return pagePool[idleIdx];
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public  void transactionComplete(TransactionId tid) throws IOException {
        lockManager.releaseTransaction(tid);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public   boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public   void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        Set<PageId> pages = lockManager.getPageHoldedByTransaction(tid);
        for (PageId pid : pages) {
            if (!cachedPageIndex.containsKey(pid)) {
                continue;
            }
            if (commit) {
                flushPage(pid);
            } else {
                pagePool[cachedPageIndex.get(pid)] = null;
                idlePageIdx.add(cachedPageIndex.get(pid));
                cachedPageIndex.remove(pid);
                latestUsedTimestamp.remove(pid);
            }
        }
        transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public  void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> pages = file.insertTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from insertTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page page = file.deleteTuple(tid, t);
        page.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (int idx : cachedPageIndex.values()) {
            flushPage(pagePool[idx].getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        Page page = pagePool[cachedPageIndex.get(pid)];
        if (page.isDirty() != null) {
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        if (idlePageIdx.size() != 0) {
            throw new DbException("evict must meet condition : not more slots");
        }
        Page leastRecentlyUsedPage = null;
        int leastTimestamp = timestamp;
        for (Page page : pagePool) {
            if (page.isDirty() != null) {
                continue;
            }
            if (leastTimestamp > latestUsedTimestamp.get(page.getId())) {
                leastRecentlyUsedPage = page;
                leastTimestamp = latestUsedTimestamp.get(page.getId());
            }
        }
        try {
            flushPage(leastRecentlyUsedPage.getId());
        } catch (Exception e) {
            throw new DbException("flush page failed.");
        }
        int idx = cachedPageIndex.get(leastRecentlyUsedPage.getId());
        idlePageIdx.add(idx);
        pagePool[idx] = null;
        cachedPageIndex.remove(leastRecentlyUsedPage.getId());
        latestUsedTimestamp.remove(leastRecentlyUsedPage.getId());
    }

}
