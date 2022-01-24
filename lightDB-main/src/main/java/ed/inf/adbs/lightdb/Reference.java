package ed.inf.adbs.lightdb;

import java.util.HashMap;

/**
 * A singleton class for saving alias.
 * Mapping from the reference table name to the real table name.
 * When there is no alias, the reference and real table names are the same.
 */
public final class Reference {

    private static Reference referenceUnique = null;
    private static HashMap<String, String> tbRef2tbName = new HashMap<>();

    private Reference(HashMap<String, String> tbRef2tbName){
        Reference.tbRef2tbName = tbRef2tbName;
    }

    public static Reference getInstance(HashMap<String, String> tbRef2tbName){
        if (referenceUnique == null)
            referenceUnique = new Reference(tbRef2tbName);
        return referenceUnique;
    }

    public static HashMap<String, String> getTbRef2tbName() {
        return tbRef2tbName;
    }
}
