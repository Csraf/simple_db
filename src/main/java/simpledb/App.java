package simpledb;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Hello world!
 *
 */
public class App 
{


    private static void up(int n){
        switch (n){
            case 1:
                System.out.println("1");
                break;
            case 2:
                System.out.println("2");
                break;
            case 3:
                System.out.println("3");
                break;
            default:
                System.out.println("4");
        }
    }

    public static void main( String[] args ) throws IOException {

//        up(1);

        List<Integer> integers = new ArrayList<>();

//        // create a 3-column table schema
//        Type[] types = {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
//        String[] names = {"field0", "field1", "field2"};
//        TupleDesc tupleDesc = new TupleDesc(types, names);
//
//        // create the table, associate it with some_data_file.dat
//        // and tell the catalog about the schema of this table.
//
//        HeapFile heapFile = new HeapFile(new File("some_data_file.dat"), tupleDesc);
//        Database.getCatalog().addTable(heapFile, "test");
//
//        // construct the query : we use a simple SeqScan, which
//        // spoonfeeds tuples via its iterator.
//
//        TransactionId tid = new TransactionId();
//        SeqScan scan = new SeqScan(tid, heapFile.getId());
//
//        try {
//            scan.open();
//            System.out.println(scan.hasNext());
//            while(scan.hasNext()){
//                Tuple tup = scan.next();
//                System.out.println(tup);
//            }
//            scan.close();
//            Database.getBufferPool().transactionComplete(tid);
//        } catch (DbException e) {
//            e.printStackTrace();
//        } catch (TransactionAbortedException e) {
//            e.printStackTrace();
//        }

    }
}
