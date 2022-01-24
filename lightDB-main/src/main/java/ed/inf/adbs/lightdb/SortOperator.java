package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

/**
 * Sort all tuples according to the orderByElementList (and more).
 *
 * orderAll is an indicator showing whether tuples shall be ordered according to all the fields.
 * It is not necessary when distinct is not present.
 *
 * When it's distinct, in order to hand over the fully sorted list of tuples, SortOperator will
 * not only consider the orderByElementList, but also add all other fields at the end of the list of fields to be ordered by.
 */
public class SortOperator extends Operator {

    private final Operator operator;
    private final List<OrderByElement> orderByElementList;
    private ArrayList<Tuple> tuples2sort;
    private Integer pos = -1;
    private boolean sorted = false;
    private final boolean orderAll;

    public SortOperator(List<OrderByElement> orderByElementList, Operator operator, Boolean orderAll) {
        this.orderAll = orderAll;
        this.orderByElementList = orderByElementList;
        this.operator = operator;
    }

    @Override
    public void reset() {
        pos = -1;
    }

    /**
     * If it is called directly, it should guarantee that tuples are sorted. (if not, call dump())
     * pos indicates the current position of the tuple in the sorted list.
     * @return the next tuple in a list of sorted tuples.
     */
    @Override
    public Tuple getNextTuple() {
        if (!sorted)
            dump();
        if (pos + 1 >= tuples2sort.size())
            return null;
        pos += 1;
        return tuples2sort.get(pos);
    }

    /**
     * Different from other Operators, this dump shall be fully processed before fully processing any getNextTuple.
     *
     * It fetches all tuples from the child operator. (calling dump())
     *
     * Preprocess the orderByElementList to create a list of indices in the current schema.
     * If it needs to sort on all fields, append the rest of the indices to the "orderFieldList".
     *
     * "sorted" is a flag showing whether dump is fully processed.
     *
     * @return all tuples in a sorted order.
     */
    @Override
    public ArrayList<Tuple> dump() {
        /* get all tuples */
        tuples2sort = operator.dump();
        ArrayList<String> currentSchema = tuples2sort.get(0).getTupleSchema();

        List<Integer> orderFieldList = new ArrayList<>();
        if (orderByElementList != null){
            for (OrderByElement orderByElement: orderByElementList){
                Expression expression = orderByElement.getExpression();
                if (expression instanceof Column){
                    String tbRef = ((Column) expression).getTable().getName();
                    String colName = ((Column) expression).getColumnName();
                    orderFieldList.add(currentSchema.indexOf(tbRef+"#"+colName));
                }
            }
        }
        if (orderAll){
            /* adding the rest of fields to be ordered by */
            for (String thisField: currentSchema){
                Integer thisFNumber = currentSchema.indexOf(thisField);
                if (!orderFieldList.contains(thisFNumber))
                    orderFieldList.add(thisFNumber);
            }
        }

        tuples2sort.sort((o1, o2) -> {
            int indicator = 0;
            for (Integer orderNumber : orderFieldList) {
                Integer o1V = o1.getTupleValues().get(orderNumber);
                Integer o2V = o2.getTupleValues().get(orderNumber);
                indicator = o1V.compareTo(o2V);
                if (indicator != 0)
                    break;
            }
            return indicator;
        });
        sorted = true;

        return tuples2sort;
    }
}