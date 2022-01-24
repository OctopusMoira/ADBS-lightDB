package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.Stack;

/**
 * It is to evaluate a tuple against certain expressions.
 *
 * "pass" indicates whether the tuple passes the inspection of the expression.
 * For each comparison expression, if the tuple doesn't satisfy, "pass" will be set to false.
 *
 * For one expression, when it encounters Column,
 * the value in the corresponding location in the tuple will be fetched.
 * Such a value will be pushed into integerStack. So will all LongValue type expression.
 *
 * The integerStack will be at most the size of two.
 * Under the correct expression, when the integerStack is "full",
 * we encounter a comparison expression and pop out those two values to compare.
 */
public class EvaluateExpr extends ExpressionDeParser {

    private ArrayList<String> schema;
    private ArrayList<Integer> values;
    private Stack<Integer> integerStack = new Stack<>();
    private boolean pass = true;

    public EvaluateExpr(Tuple tuple) {
        schema = tuple.getTupleSchema();
        values = tuple.getTupleValues();
    }

    public EvaluateExpr() {
    }

    @Override
    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
    }

    @Override
    public void visit(LongValue longValue) {
        super.visit(longValue);
        integerStack.push((int)longValue.getValue());
    }

    @Override
    public void visit(Column tableColumn) {
        super.visit(tableColumn);
        String tbRef = tableColumn.getTable().getName();
        String colName = tableColumn.getColumnName();
        Integer value = values.get(schema.indexOf(tbRef+"#"+colName));
        integerStack.push(value);
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) != 0)
            pass = false;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) == 0)
            pass = false;
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) != 1)
            pass = false;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) == -1)
            pass = false;
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) != -1)
            pass = false;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        Integer right = integerStack.pop();
        Integer left = integerStack.pop();
        if (left.compareTo(right) == 1)
            pass = false;
    }

    public Boolean getPass(){
        return pass;
    }

}
