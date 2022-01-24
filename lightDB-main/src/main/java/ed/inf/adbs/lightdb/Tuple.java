package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/**
 * Saves information including schema and values for each tuple.
 */
public class Tuple {

    private final ArrayList<String> tupleSchema;
    private final ArrayList<Integer> tupleValues;

    public Tuple(ArrayList<String> schema, ArrayList<Integer> values) {
        tupleSchema = schema;
        tupleValues = values;
    }

    public ArrayList<Integer> getTupleValues() {
        return tupleValues;
    }

    public ArrayList<String> getTupleSchema() {
        return tupleSchema;
    }

}
