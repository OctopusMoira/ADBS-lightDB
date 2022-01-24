package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * A singleton class for saving database information.
 * Information includes the file directory and schema for each table.
 */
public final class Catalog {

    private static Catalog catalogUnique = null;

    private static HashMap<String, String> name2addr = new HashMap<>();
    private static HashMap<String, ArrayList<String>> name2schema = new HashMap<>();

    private Catalog(String dbDir) {
        try {
            Scanner scanner = new Scanner(new File(dbDir+"/schema.txt"));
            while(scanner.hasNextLine()){
                String oneSchema = scanner.nextLine();
                System.out.println(oneSchema);
                String[] split = oneSchema.split("\\s", 2);
                name2addr.put(split[0], dbDir+"/data/"+split[0]+".csv");
                ArrayList<String> colnames = new ArrayList<>(Arrays.asList(split[1].split("\\s")));
                name2schema.put(split[0], colnames);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Catalog getInstance(String dbDir){
        if (catalogUnique == null)
            catalogUnique = new Catalog(dbDir);
        return catalogUnique;
    }

    public static String getFileAdr(String tablename){
        return name2addr.get(tablename);
    }

    public static ArrayList<String> getTableSchema(String tablename){
        return name2schema.get(tablename);
    }

}
