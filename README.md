# lightDB
## Explanation: join conditions extraction from the WHERE clause.

In the Class QueryPlan, WHERE clause is preprocessed in function preprocessWhere(), which is called in the constructor.

The extraction explanation starts from Line 99.

Another thing regarding processing join conditions in the actual joining process is removing the mapping for certain set after those conditions are used.

It starts in the same file from Line 281 and 296.

## Other information needed.

Everything starts from "lightDB".

Then query tree logic in "QueryPlan" -- extract information from statement, reconstruct information to be easily processed, dump query results.
Information reconstruction includes extracting alias, processing conditions.

"Operator" is extended by "ScanOperator", "SelectOperator", "JoinOperator", "ProjectOperator", "SortOperator" and "DuplicateEliminationOperator".
Among them, Sort is quite different in the idea of dump() and getNextTuple().

"Tuple" saves tuple information -- tuple schema and value.
The schema of each tuple is changed to "tableReference#columnName" to be unique, especially useful in self-join.

"Catalog" saves table information -- table directory and schema.

"Reference" saves table information -- alias.

"EvaluateExpr" is an ExpressionDeparser that's been used several times -- Join and Select.
