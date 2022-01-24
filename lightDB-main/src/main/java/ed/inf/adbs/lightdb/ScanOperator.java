package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Scanning tuples from a table file.
 */
public class ScanOperator extends Operator{

    private String btRef;
    private ArrayList<String> newSchema = new ArrayList<>();
    private Scanner scanner = null;

    /**
     * Prepare scanning.
     * Get file directory from Catalog and create scanner.
     * Get table schema from Catalog and change the schema to
     * make each column distinguishable in the whole database.
     * @param btRef Reference name of a table
     */
    public ScanOperator(String btRef){
        /* opens a file scan on the appropriate data file */
        this.btRef = btRef;
        try {
            scanner = new Scanner(new File(Catalog.getFileAdr(Reference.getTbRef2tbName().get(btRef))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ArrayList<String> tbSchema = Catalog.getTableSchema(Reference.getTbRef2tbName().get(btRef));
        for (String string: tbSchema)
            newSchema.add(btRef+"#"+string);
    }

    public Tuple getNextTuple(){
        /* read next tuple from the file */
        /* and return the next tuple */
        if (scanner.hasNextLine()){
            String nextLine = scanner.next();
            String[] strValues = nextLine.split(",");
            ArrayList<Integer> values = new ArrayList<>(strValues.length);
            for (String str: strValues){
                values.add(Integer.valueOf(str));
            }
            return new Tuple(newSchema, values);
        }
        return null;
    }

    public void reset(){
        /* to read from the start of the file */
        try {
            scanner = new Scanner(new File(Catalog.getFileAdr(Reference.getTbRef2tbName().get(btRef))));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<Tuple> dump(){
        return super.dump();
    }
}
