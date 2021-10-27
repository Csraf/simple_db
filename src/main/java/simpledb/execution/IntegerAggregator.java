package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final Type gbfieldtype;
    private TupleDesc td;
    private final Op what;
    private HashMap<Field, IntegerAggregatorResult> groupMap= new HashMap<>();
    private static final Field NO_GROUP = new IntField(-1);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        else{
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggField = tup.getField(afield);
        Integer newVal = ((IntField)aggField).getValue();
        if(gbfield == NO_GROUPING){
            doMergeTupleIntoGroup(NO_GROUP, newVal);
        }
        else{
            Field groupField = tup.getField(gbfield);
            doMergeTupleIntoGroup(groupField, newVal);
        }
    }

    private void doMergeTupleIntoGroup(Field groupField , Integer newVal){
        if(groupMap.containsKey(groupField)){
            groupMap.get(groupField).merge(newVal);
        }
        else{
            IntegerAggregatorResult iar = new IntegerAggregatorResult(this.what);
            iar.merge(newVal);
            groupMap.put(groupField, iar);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Field f : groupMap.keySet()) {
            IntegerAggregatorResult iar = groupMap.get(f);
            Tuple tuple = new Tuple(td);
            if(gbfield == NO_GROUPING){
                tuple.setField(0,new IntField((int) iar.curVal));
            }
            else{
                tuple.setField(0, f);
                tuple.setField(1, new IntField((int) iar.curVal));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    class IntegerAggregatorResult {
        private int count = 0;
        private float curVal = 0;
        private final Op what;

        public IntegerAggregatorResult(Op what){
            this.what = what;
        }

        public void merge(int newVal){
            count++;
            if(count == 1){
                if(what == Op.COUNT){
                    curVal = count;
                }
                else curVal = newVal;
                return;
            }

            switch (what){
                case AVG:
                    curVal = (curVal*(count-1)+newVal) / count;
                    break;
                case MAX:
                    curVal = Math.max(curVal, newVal);
                    break;
                case MIN:
                    curVal = Math.min(curVal, newVal);
                    break;
                case SUM:
                    curVal = curVal + newVal;
                    break;
                case COUNT:
                    curVal = count;
            }
        }
    }
}
