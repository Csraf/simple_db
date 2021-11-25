package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pgNo = pid.getPageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] data = new byte[pageSize];
        HeapPage page = null;
        try {
            FileInputStream fis = new FileInputStream(f);
            fis.skip(pgNo*pageSize);
            fis.read(data);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pgNo = page.getId().getPageNumber();
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] data = page.getPageData();
        RandomAccessFile rf = new RandomAccessFile(f, "rw");
        rf.seek(pgNo*pageSize);
        rf.write(data);
        rf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        List<Page> pages = new ArrayList<>();
        int tableId = getId();
        int numPages = numPages();
        int pageSize = BufferPool.getPageSize();

        for (int i = 0; i <numPages; i++) {
            // 迭代当前页面，寻找一个空槽，然后将页面刷新回磁盘
            HeapPageId pid = new HeapPageId(tableId, i);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, null);
            int numEmptySlots = page.getNumEmptySlots();
            if(numEmptySlots > 0) {
                page.insertTuple(t);
                pages.add(page);
                writePage(page); // 刷新回到磁盘
                return pages;
            }
        }

        // 创建新的一页
        HeapPageId newPid = new HeapPageId(tableId, numPages);
        HeapPage newPage = new HeapPage(newPid, new byte[pageSize]);
        newPage.insertTuple(t);
        pages.add(newPage);
        writePage(newPage);

        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> pages = new ArrayList<>();
        RecordId recordId = t.getRecordId();
        PageId pageId = recordId.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, null);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator extends AbstractDbFileIterator{

        private TransactionId tid;
        private HeapFile f;

        private int pgNo = 0;
        private HeapPage curPage = null;
        private Iterator<Tuple> it = null;

        public HeapFileIterator(HeapFile f, TransactionId tid){
            this.f = f;
            this.tid = tid;
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if(it == null) return null;
            while(!it.hasNext() && pgNo < f.numPages()-1){ // 读取下一页
                pgNo += 1;
                curPage = (HeapPage) Database.getBufferPool().
                        getPage(tid, new HeapPageId(f.getId(), pgNo),null);
                it = curPage.iterator();
            }
            if(pgNo == f.numPages()) return null;
            Tuple t = it.next();
            return t;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgNo = 0;
            curPage = (HeapPage) Database.getBufferPool().
                    getPage(tid, new HeapPageId(f.getId(), pgNo),null);
            it = curPage.iterator();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            curPage = null;
            it = null;
            pgNo = f.numPages();
        }
    }
}

