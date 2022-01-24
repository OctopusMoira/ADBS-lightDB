package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

/**
 * Join two tables. (Tuples from two child operators.)
 *
 * "expression" is the join condition or null.
 * If null, do cross product. (simply, return directly)
 *
 * "currentLeft" is to record a tuple from the left operator.
 * It's set still until all tuples from the right operator are returned.
 */
public class JoinOperator extends Operator{

    private final Operator operatorLeft;
    private final Operator operatorRight;
    private Tuple currentLeft;
    private final Expression expression;

    public JoinOperator(Expression expression, Operator operatorLeft, Operator operatorRight) {
        this.operatorLeft = operatorLeft;
        this.operatorRight = operatorRight;
        this.currentLeft = operatorLeft.getNextTuple();
        this.expression = expression;
    }

    @Override
    public Tuple getNextTuple() {

        /* entire tuples end */
        if (currentLeft == null)
            return null;

        Tuple tupleRight = operatorRight.getNextTuple();

        /* exists a potential new tuple */
        while (tupleRight != null){
            ArrayList<String> newschema = new ArrayList<>();
            newschema.addAll(currentLeft.getTupleSchema());
            newschema.addAll(tupleRight.getTupleSchema());

            ArrayList<Integer> newvalue = new ArrayList<>();
            newvalue.addAll(currentLeft.getTupleValues());
            newvalue.addAll(tupleRight.getTupleValues());

            /* construct a new tuple using new schema and concatenated values */
            Tuple tuple = new Tuple(newschema, newvalue);

            /* cross product */
            if (expression == null)
                return tuple;

            /* evaluate the new tuple */
            EvaluateExpr evaluateExpr = new EvaluateExpr(tuple);
            expression.accept(evaluateExpr);

            if (evaluateExpr.getPass())
                return tuple;
            tupleRight = operatorRight.getNextTuple();
        }

        currentLeft = operatorLeft.getNextTuple();
        if (currentLeft != null){
            operatorRight.reset();
            return getNextTuple();
        }

        return null;
    }

    @Override
    public void reset() {
        super.reset();
        operatorLeft.reset();
        operatorRight.reset();
        currentLeft = null;
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}
