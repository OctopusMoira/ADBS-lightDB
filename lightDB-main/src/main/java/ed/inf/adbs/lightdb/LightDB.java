package ed.inf.adbs.lightdb;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.stream.Collectors;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Lightweight in-memory database system.
 */
public class LightDB {

	/**
	 * Call Catalog to save database information.
	 * Enter query execution block.
	 * @param args database, input and output file directory
	 */
	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		Catalog.getInstance(databaseDir);
		queryExecute(inputFile, outputFile);
	}

	/**
	 * Extract statement from input file.
	 * Call QueryPlan to process the query.
	 * Write returned query results into file.
	 * @param inputFile Relative directory for input query file
	 * @param outputFile Relative directory for query result output file
	 */
	public static void queryExecute(String inputFile, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				QueryPlan queryPlan = new QueryPlan(statement);
				ArrayList<Tuple> results = queryPlan.dump();
				if (results != null && results.size() != 0)
					System.out.println(results.get(0).getTupleSchema());

				FileWriter fileWriter = new FileWriter(outputFile);
				if (results != null && results.size() != 0){
					for (Tuple tuple: results){
						String oneTuple = tuple.getTupleValues().stream().map(Object::toString).collect(Collectors.joining(","));
						fileWriter.append(oneTuple);
						fileWriter.append("\n");
						System.out.println(oneTuple);
					}
				}
				fileWriter.flush();
			}
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
	}
}
