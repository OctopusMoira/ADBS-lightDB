package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/**
 * Abstract class for operators.
 */
public abstract class Operator {

    public Operator() {

    }

    public Tuple getNextTuple() {
        return null;
    }

    public void reset() {}

    /**
     * Calling getNextTuple to get all tuples.
     * @return A list of all tuples.
     */
    public ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (true){
            Tuple tuple = getNextTuple();
            if (tuple == null)
                break;
            tuples.add(tuple);
        }
        return tuples;
    }
}
