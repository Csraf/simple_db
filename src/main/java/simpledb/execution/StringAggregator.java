package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final int afield;
    private final Type gbfieldtype;
    private final Op what;
    private final TupleDesc td;
    private Map<Field, StringAggregatorResult> groupMap = new HashMap<>();
    private static final Field NO_GROUP = new IntField(-1);



    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        if(what != Op.COUNT) throw new IllegalArgumentException("Type String has only COUNT aggregator");

        if(gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        else{
            td = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String newVal = ((StringField) tup.getField(afield)).getValue();
        if(afield == NO_GROUPING){
            doMergeTupleIntoGroup(NO_GROUP, newVal);
        }
        else{
            doMergeTupleIntoGroup(tup.getField(gbfield), newVal);
        }
    }

    private void doMergeTupleIntoGroup(Field groupField, String newVal){
        if(groupMap.containsKey(groupField)){
            StringAggregatorResult sar = groupMap.get(groupField);
            sar.merge(newVal);
        }
        else{
            StringAggregatorResult sar = new StringAggregatorResult(what);
            sar.merge(newVal);
            groupMap.put(groupField, sar);
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        List <Tuple> tuples = new ArrayList<>();
        for (Field f : groupMap.keySet()) {
            StringAggregatorResult sar = groupMap.get(f);
            Tuple tuple = new Tuple(td);
            if(gbfield == NO_GROUPING){
                tuple.setField(0, new IntField((int) sar.curVal));
            }
            else {
                tuple.setField(0, f);
                tuple.setField(1, new IntField((int) sar.curVal));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

    class StringAggregatorResult {
        private float curVal = 0;
        private int count = 0;
        private final Op what;

        public StringAggregatorResult(Op what){
            this.what = what;
        }

        public void merge(String newVal){
            count++;
            switch (what){
                case COUNT:
                    curVal = count;
                    break;
            }
        }
    }

}
