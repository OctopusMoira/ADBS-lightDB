package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/**
 * Eliminate duplication in an "ordered" stream of tuples.
 *
 * "formerTuple" is to record the tuple visited last time.
 *
 * When the current tuple is different from the formerTuple,
 * it is the first occurrence of such tuples. Thus it shall be returned.
 *
 */
public class DuplicateEliminationOperator extends Operator{

    private final Operator operator;
    private Tuple formerTuple;

    public DuplicateEliminationOperator(Operator operator) {
        this.operator = operator;
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }

    @Override
    public Tuple getNextTuple() {

        Tuple tuple = operator.getNextTuple();
        if (tuple == null)
            return null;

        /* very first tuple */
        if (formerTuple == null){
            formerTuple = tuple;
            return tuple;
        }

        /* check whether identical */
        if (formerTuple.getTupleValues().equals(tuple.getTupleValues()))
            return getNextTuple();

        /* not identical. first of its kind */
        formerTuple = tuple;
        return tuple;
    }

    @Override
    public void reset() {
        super.reset();
        operator.reset();
    }
}
