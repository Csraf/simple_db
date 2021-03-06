package simpledb.execution;

import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator[] child = new OpIterator[2];


    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child[0] = child1;
        this.child[1] = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child[0].getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child[1].getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child[0].getTupleDesc(), child[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child[0].open();
        child[1].open();
        super.open();
    }

    public void close() {
        // some code goes here
        child[0].close();
        child[1].close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        OpIterator leftOperator = child[0];
        OpIterator rightOperator = child[1];
        while(copy != null || leftOperator.hasNext()){
            Tuple leftTuple;
            if(copy != null) leftTuple = copy;
            else leftTuple = leftOperator.next();
            while(rightOperator.hasNext()){
                Tuple rightTuple = rightOperator.next();
                if(p.filter(leftTuple, rightTuple)) {
                    copy = leftTuple;
                    return mergeTuple(leftTuple, rightTuple);
                }
            }
            copy = null;
            rightOperator.rewind();
        }
        return null;
    }

    private Tuple copy = null;

    /**
     * ??????????????????
     */
    private Tuple mergeTuple(Tuple t1, Tuple t2){
        Tuple tuple = new Tuple(getTupleDesc());
        Iterator<Field> it1 = t1.fields();
        Iterator<Field> it2 = t2.fields();
        int index = 0;
        while(it1.hasNext()){
            Field f = it1.next();
            tuple.setField(index, f);
            index++;
        }
        while(it2.hasNext()){
            Field f = it2.next();
            tuple.setField(index, f);
            index++;
        }
        return tuple;
    }


    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return child;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children;
    }

}
