package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;

import java.util.ArrayList;

/**
 * Cast selection condition on each tuple,
 * using an ExpressionDeparser -- EvaluateExpr,
 * and return tuple that qualifies.
 */
public class SelectOperator extends Operator {

    private final Expression expression;
    private final ScanOperator scanOperator;

    public SelectOperator(Expression expression, ScanOperator scanOperator) {
        this.scanOperator = scanOperator;
        this.expression = expression;
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = scanOperator.getNextTuple();
        while (tuple != null){
            EvaluateExpr evaluateExpr = new EvaluateExpr(tuple);
            expression.accept(evaluateExpr);
            if (evaluateExpr.getPass())
                return tuple;
            tuple = scanOperator.getNextTuple();
        }
        return null;
    }

    @Override
    public void reset() {
        scanOperator.reset();
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}
