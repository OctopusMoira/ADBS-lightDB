package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * To select certain columns of a table.
 *
 * "selectItemList" is processed in the constructor
 * to create the real schema for all the tuples, which is saved in overSchema.
 */
public class ProjectOperator extends Operator{

    private final Operator operator;
    private ArrayList<String> overSchema = new ArrayList<>();

    public ProjectOperator(List<SelectItem> selectItemList, Operator operator) {
        this.operator = operator;
        /* construct the "overSchema" here because it is the same under one projection */
        for (SelectItem selectItem: selectItemList){
            if (selectItem instanceof SelectExpressionItem){
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if (expression instanceof Column){
                    String tbRef = ((Column) expression).getTable().getName();
                    String colName = ((Column) expression).getColumnName();
                    overSchema.add(tbRef+"#"+colName);
                }
            }
        }
    }

    @Override
    public Tuple getNextTuple() {
        Tuple tuple = operator.getNextTuple();

        if (tuple == null)
            return null;

        ArrayList<Integer> values = new ArrayList<>();

        for (String field: overSchema)
            values.add(tuple.getTupleValues().get(tuple.getTupleSchema().indexOf(field)));

        return new Tuple(overSchema, values);
    }

    @Override
    public void reset() {
        operator.reset();
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}
