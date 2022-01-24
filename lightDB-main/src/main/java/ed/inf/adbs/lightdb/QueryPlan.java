package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.*;

/**
 * Construct a query plan for one statement and return query results.
 */
public class QueryPlan {

    private Distinct distinct;
    private List<SelectItem> selectItem;
    private FromItem fromItem;
    private List<Join> joinList;
    private Expression where;
    private List<OrderByElement> orderByElementList;

    /**
     * In preprocessing the where clause, comparison expressions that are only of LongValue will be calculated.
     * If such expression is already false, the results will be empty anyway.
     */
    private Boolean alreayEmpty = false;

    /**
     * table name and reference related.
     */
    private HashMap<String, String> tbRef2tbName = new HashMap<>();
    private String baseRef;
    private List<String> orderedJoinRef = new ArrayList<>();

    /**
     * seperated conditions for join and selection
     */
    private Map<Set<String>, Expression> whereForJoin = new HashMap<>();
    private Map<String, Expression> whereForSelect = new HashMap<>();

    /**
     * Raw extractions from the statement.
     * Preprocess from (for alias) and where (for conditions)
     */
    public QueryPlan(Statement statement) {
        Select select = (Select) statement;
        SelectBody selectBody = select.getSelectBody();
        if (selectBody instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) selectBody;
            distinct = plainSelect.getDistinct();
            selectItem = plainSelect.getSelectItems();
            fromItem = plainSelect.getFromItem();
            joinList = plainSelect.getJoins();
            where = plainSelect.getWhere();
            orderByElementList = plainSelect.getOrderByElements();

            System.out.println("          distinct : "+distinct);
            System.out.println("        selectItem : "+selectItem);
            System.out.println("          fromItem : "+fromItem);
            System.out.println("          joinList : "+joinList);
            System.out.println("             where : "+where);
            System.out.println("orderByElementList : "+orderByElementList);
        }

        preprocessFrom();
        if (where != null)
            preprocessWhere();

    }

    /**
     * Extract alias from the fromItem and joinList.
     * If table has no alias, the alias representing the table will be the table name itself.
     * Such information will be saved in a singleton class called Reference.
     */
    private void preprocessFrom(){
        String[] splited = fromItem.toString().split("\\s");
        String tempRef = (splited.length==1)?splited[0]:splited[1];
        tbRef2tbName.put(tempRef,splited[0]);
        baseRef = tempRef;
        if (joinList != null){
            for (Join join: joinList){
                splited = join.getRightItem().toString().split("\\s");
                tempRef = (splited.length==1)?splited[0]:splited[1];
                tbRef2tbName.put(tempRef,splited[0]);
                orderedJoinRef.add(tempRef);
            }
        }
        Reference.getInstance(tbRef2tbName);
        System.out.println("      tbRef2tbName : "+tbRef2tbName);
        System.out.println("    orderedJoinRef : "+orderedJoinRef);
    }

    /**
     * Extract selection and join conditions from where.
     * Three possible expressions.
     *
     * 1) Comparing two LongValue.
     * It can be calculated and change the "alreadyEmpty".
     *
     * 2) Comparing two column values.
     * It belongs to join condition. It will be saved in a mapping
     * from a set (containing the left and right tables) to an expression
     * of all such expressions. (create ANDExpression when necessary)
     *
     * 3) Comparing a column value to a LongValue.
     * It belongs to select conditions. It's saved in the similar way as 2).
     * Creating a mapping from a table to an expression. (AND if necessary)
     */
    private void preprocessWhere(){

        ExpressionDeParser expressionDeParser = new ExpressionDeParser(){
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);

                Expression leftExpression = andExpression.getLeftExpression();
                Expression rightExpression = andExpression.getRightExpression();

                /**
                 * Every leftExpression is processed.
                 */
                if (leftExpression instanceof ComparisonOperator){
                    Expression expressionLeft = ((ComparisonOperator) leftExpression).getLeftExpression();
                    Expression expressionRight = ((ComparisonOperator) leftExpression).getRightExpression();
                    if (expressionLeft instanceof LongValue && expressionRight instanceof LongValue){
                        /* Evaluate both LongValue and do not save. */
                        EvaluateExpr evaluateExpr = new EvaluateExpr();
                        leftExpression.accept(evaluateExpr);
                        if (!evaluateExpr.getPass()){
                            alreayEmpty = true;
                        }
                    } else if (expressionLeft instanceof Column && expressionRight instanceof Column){
                        /* Process join condition to be saved in a mapping. Key is a set of tables.*/
                        Set<String> tbSet = new HashSet<>();
                        tbSet.add(((Column) expressionLeft).getTable().getName());
                        tbSet.add(((Column) expressionRight).getTable().getName());
                        /* The mapping value is null at first.
                        * For the rest expressions, they shall be concatenated
                        * with the existing value using ANDExpression.*/
                        if (whereForJoin.get(tbSet) == null){
                            whereForJoin.put(tbSet, leftExpression);
                        } else {
                            AndExpression andExpression1 = new AndExpression();
                            andExpression1.withRightExpression(whereForJoin.get(tbSet));
                            andExpression1.withLeftExpression(leftExpression);
                            whereForJoin.put(tbSet, andExpression1);
                        }
                    } else {
                        /* Selection condition and saved in mapping. Key is a table.*/
                        String tbRef = null;
                        if (expressionLeft instanceof Column)
                            tbRef = ((Column) expressionLeft).getTable().getName();
                        if (expressionRight instanceof Column)
                            tbRef = ((Column) expressionRight).getTable().getName();
                        if (whereForSelect.get(tbRef) == null)
                            whereForSelect.put(tbRef, leftExpression);
                        else{
                            AndExpression andExpression1 = new AndExpression();
                            andExpression1.withRightExpression(whereForSelect.get(tbRef));
                            andExpression1.withLeftExpression(leftExpression);
                            whereForSelect.put(tbRef, andExpression1);
                        }
                    }
                }

                /**
                 * Only the last right expression is processed, which is a ComparisonOperator.
                 */
                if (rightExpression instanceof ComparisonOperator){
                    Expression expressionLeft = ((ComparisonOperator) rightExpression).getLeftExpression();
                    Expression expressionRight = ((ComparisonOperator) rightExpression).getRightExpression();
                    if (expressionLeft instanceof LongValue && expressionRight instanceof LongValue){
                        EvaluateExpr evaluateExpr = new EvaluateExpr();
                        rightExpression.accept(evaluateExpr);
                        if (!evaluateExpr.getPass()){
                            alreayEmpty = true;
                        }
                    } else if (expressionLeft instanceof Column && expressionRight instanceof Column){
                        Set<String> tbSet = new HashSet<>();
                        tbSet.add(((Column) expressionLeft).getTable().getName());
                        tbSet.add(((Column) expressionRight).getTable().getName());
                        if (whereForJoin.get(tbSet) == null){
                            whereForJoin.put(tbSet, rightExpression);
                        } else {
                            AndExpression andExpression1 = new AndExpression();
                            andExpression1.withRightExpression(whereForJoin.get(tbSet));
                            andExpression1.withLeftExpression(rightExpression);
                            whereForJoin.put(tbSet, andExpression1);
                        }
                    } else {
                        String tbRef = null;
                        if (expressionLeft instanceof Column)
                            tbRef = ((Column) expressionLeft).getTable().getName();
                        if (expressionRight instanceof Column)
                            tbRef = ((Column) expressionRight).getTable().getName();
                        if (whereForSelect.get(tbRef) == null)
                            whereForSelect.put(tbRef, rightExpression);
                        else{
                            AndExpression andExpression1 = new AndExpression();
                            andExpression1.withRightExpression(whereForSelect.get(tbRef));
                            andExpression1.withLeftExpression(rightExpression);
                            whereForSelect.put(tbRef, andExpression1);
                        }
                    }
                }

            }
        };

        /**
         * "where" can be ANDExpression or a single comparison expression.
         */
        if (where instanceof AndExpression)
            where.accept(expressionDeParser);
        else
            if (where instanceof ComparisonOperator){
                Expression expressionLeft = ((ComparisonOperator) where).getLeftExpression();
                Expression expressionRight = ((ComparisonOperator) where).getRightExpression();
                if (expressionLeft instanceof LongValue && expressionRight instanceof LongValue){
                    EvaluateExpr evaluateExpr = new EvaluateExpr();
                    where.accept(evaluateExpr);
                    if (!evaluateExpr.getPass()){
                        alreayEmpty = true;
                    }
                } else if (expressionLeft instanceof Column && expressionRight instanceof Column){
                    Set<String> tbSet = new HashSet<>();
                    tbSet.add(((Column) expressionLeft).getTable().getName());
                    tbSet.add(((Column) expressionRight).getTable().getName());
                    if (whereForJoin.get(tbSet) == null){
                        whereForJoin.put(tbSet, where);
                    } else {
                        AndExpression andExpression1 = new AndExpression();
                        andExpression1.withRightExpression(whereForJoin.get(tbSet));
                        andExpression1.withLeftExpression(where);
                        whereForJoin.put(tbSet, andExpression1);
                    }
                } else {
                    String tbRef = null;
                    if (expressionLeft instanceof Column)
                        tbRef = ((Column) expressionLeft).getTable().getName();
                    if (expressionRight instanceof Column)
                        tbRef = ((Column) expressionRight).getTable().getName();
                    if (whereForSelect.get(tbRef) == null)
                        whereForSelect.put(tbRef, where);
                    else{
                        AndExpression andExpression1 = new AndExpression();
                        andExpression1.withRightExpression(whereForSelect.get(tbRef));
                        andExpression1.withLeftExpression(where);
                        whereForSelect.put(tbRef, andExpression1);
                    }
                }
            }

        System.out.println("      whereForJoin : "+whereForJoin);
        System.out.println("    whereForSelect : "+whereForSelect);

    }

    /**
     * The logic of query tree.
     *
     * "operatorLeft" records the current top structure of operator in a query tree. (from bottom to top)
     * When it is fully constructed, the query process will be from top to bottom.
     *
     * @return Final list of query results.
     */
    public ArrayList<Tuple> dump(){

        /**
         * If alreadyEmpty, no need to process further.
         */
        if (alreayEmpty)
            return null;

        /**
         * "currentRefSet" is used to save a set of tables that HAVE BEEN joined.
         */
        Set<String> currentRefSet = new HashSet<>();

        /**
         * Always "start" with a base ScanOperator. (In line with the deep left join method)
         * For every ScanOperator, it is possible to have a selectionOperator on top of it.
         * The selection condition is saved in whereForSelect.
         */
        ScanOperator scanOperator = new ScanOperator(baseRef);
        Expression expression = whereForSelect.get(baseRef);
        Operator operatorLeft = (expression != null)? new SelectOperator(expression, scanOperator) : scanOperator;
        currentRefSet.add(baseRef);

        /**
         * For every ScanOperator apart from the base one, it associates with a JoinOperator.
         * For each JoinOperator, it checks the join condition saved in whereForJoin.
         *
         * IMPORTANT:
         * whereForSelect and whereForJoin are dynamic mappings.
         * After each selection or join, the corresponding condition will be removed from them.
         * "currentRefSet" helps distinguish which conditions are relevant to the current set of tables. (by comparing sets)
         */
        for (String joinRef: orderedJoinRef){
            /* next table is from the right operator */
            ScanOperator scanOperator1 = new ScanOperator(joinRef);
            Expression expression1 = whereForSelect.get(joinRef);
            Operator operatorRight = (expression1 != null)? new SelectOperator(expression1, scanOperator1) : scanOperator1;
            currentRefSet.add(joinRef);

            List<Expression> expressions = new ArrayList<>();
            List<Set<String>> usedJoin = new ArrayList<>();

            /* For one join, there can be several conditions. Find all relevant conditions. */
            for (Set<String> stringSet: whereForJoin.keySet()){
                if (currentRefSet.containsAll(stringSet)){
                    expressions.add(whereForJoin.get(stringSet));
                    usedJoin.add(stringSet);
                }
            }

            /* Remove already used join conditions */
            for (Set<String> stringSet: usedJoin){
                whereForJoin.remove(stringSet);
            }

            if (expressions.size() == 0){
                /* null expression -> cross product */
                operatorLeft = new JoinOperator(null, operatorLeft, operatorRight);
            } else if (expressions.size() == 1){
                /* only one join condition */
                operatorLeft = new JoinOperator(expressions.get(0), operatorLeft, operatorRight);
            } else {
                /* several join conditions. construct a final expression */
                AndExpression andExpression = new AndExpression();
                andExpression.withLeftExpression(expressions.get(0));
                andExpression.withRightExpression(expressions.get(1));
                for (Expression expression2: expressions.subList(2,expressions.size())){
                    AndExpression andExpression1 = new AndExpression();
                    andExpression1.withLeftExpression(andExpression);
                    andExpression1.withRightExpression(expression2);
                    andExpression = andExpression1;
                }
                operatorLeft = new JoinOperator(andExpression, operatorLeft, operatorRight);
            }

        }

        /**
         * After joining all the tables, there may be other operators in the order
         * of ProjectOperator, SortOperator and DuplicateEliminationOperator.
         */
        if (!(selectItem.get(0) instanceof AllColumns))
            /* not *, create Projection */
            operatorLeft = new ProjectOperator(selectItem, operatorLeft);

        if (distinct != null){
            /* distinct exists. need sorting by all fields. */
            operatorLeft = new SortOperator(orderByElementList, operatorLeft, true);
            operatorLeft = new DuplicateEliminationOperator(operatorLeft);
        } else if (orderByElementList != null){
            /* just sort on the given list */
            operatorLeft = new SortOperator(orderByElementList, operatorLeft, false);
        }

        return operatorLeft.dump();
    }

}
