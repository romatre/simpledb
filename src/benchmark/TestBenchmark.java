package benchmark;

import org.json.JSONArray;
import org.json.JSONObject;
import simpledb.metadata.MetadataMgr;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.PrintWriter;

public class TestBenchmark {

    public static JSONArray allStats = new JSONArray();


    public static void main(String args[]) throws Exception {

        generateStats("Naif, BufferSize: 8", "studentDB", "naif", 400, 8);
        generateStats("LRU, BufferSize: 8", "studentDB", "lru", 400, 8);
        generateStats("Clock, BufferSize: 8", "studentDB", "clock", 400, 8);

        generateStats("LRU, BufferSize: 2000", "studentDB", "lru", 400, 2000);
        generateStats("Clock, BufferSize: 2000", "studentDB", "clock", 400, 2000);

        generateStats("LRU, BufferSize: 20000", "studentDB", "lru", 400, 20000);
        generateStats("Clock, BufferSize: 20000", "studentDB", "clock", 400, 20000);

        PrintWriter writer = new PrintWriter("benchmark/data.json", "UTF-8");
        writer.println(allStats);
        writer.close();
    }

    private static void generateStats(String statName, String dbName, String strategy, int blockSize, int bufferSize) throws Exception {
        JSONObject result = new JSONObject();
        JSONArray data = new JSONArray();
        JSONObject singleStat;

        result.put("statName", statName);
        result.put("data", data);

        // cancello le vecchie tracce del db e inizializzo il database "studentdb"
        deleteDatabase(dbName);
        SimpleDB.init(dbName, strategy, blockSize, bufferSize);

        // ottengo l'oggetto mdMgr
        MetadataMgr mdMgr = SimpleDB.mdMgr();

        // dichiaro le variabili necessarie
        Transaction tx;
        RecordFile tcatfile;
        TableInfo ti;
        int i;

        // in questa prima transazione creo lo schema che utilizzerò per la tabella students
        System.out.println("\n\nCostruisco la tabella");
        tx = new Transaction();
        Schema sch = new Schema();
        sch.addIntField("id");
        sch.addStringField("firstName", 10);
        sch.addStringField("lastName", 10);
        sch.addStringField("gender", 1);
        mdMgr.createTable("students", sch, tx);
        tx.commit();

        // inserisco 10000 record casuali nella tabella students
        System.out.println("\n\nInserisco 10000 record casuali nella tabella students");
        tx = new Transaction();
        ti = mdMgr.getTableInfo("students", tx);

        tcatfile = new RecordFile(ti, tx);
        for(i = 0; i < 10000; i++) {
            tcatfile.insert();
            tcatfile.setInt("id", i);
            tcatfile.setString("firstName", getRandomFirstName());
            tcatfile.setString("lastName", getRandomLastName());
            tcatfile.setString("gender", getRandomGender());
        }
        tcatfile.close();
        singleStat = new JSONObject();
        singleStat.put("type", "insert10000");
        singleStat.put("data", SimpleDB.fileMgr().getAllBlockStats());
        data.put(singleStat);
        tx.commit();

        // leggo tutti i 10000 record dalla tabella students
        System.out.println("\n\nLeggo tutti i 10000 record dalla tabella students");
        tx = new Transaction();
        ti = mdMgr.getTableInfo("students", tx);
        tcatfile = new RecordFile(ti, tx);
        while (tcatfile.next()) {}
        tcatfile.close();
        singleStat = new JSONObject();
        singleStat.put("type", "scanTable");
        singleStat.put("data", SimpleDB.fileMgr().getAllBlockStats());
        data.put(singleStat);
        System.out.println(singleStat.get("data"));
        tx.commit();

        // cancello all'incirca metà dei record (soli gli studenti maschi)
        System.out.println("\n\nCancello metà dei record");
        tx = new Transaction();
        ti = mdMgr.getTableInfo("students", tx);
        tcatfile = new RecordFile(ti, tx);
        while (tcatfile.next()) {
            if(tcatfile.getString("gender").equals("M")) {
                tcatfile.delete();
            }
        }
        tcatfile.close();
        singleStat = new JSONObject();
        singleStat.put("type", "deleteHalf");
        singleStat.put("data", SimpleDB.fileMgr().getAllBlockStats());
        data.put(singleStat);
        tx.commit();

        // faccio una scansione selettiva
        System.out.println("\n\nCancello metà dei record");
        tx = new Transaction();
        ti = mdMgr.getTableInfo("students", tx);
        tcatfile = new RecordFile(ti, tx);
        while (tcatfile.next()) {
            if(tcatfile.getString("gender").equals("M")) {
                System.out.println(statName);
            }
        }
        tcatfile.close();
        singleStat = new JSONObject();
        singleStat.put("type", "selectiveScan");
        singleStat.put("data", SimpleDB.fileMgr().getAllBlockStats());
        data.put(singleStat);
        tx.commit();

        // inserisco altri 7000 record
        tx = new Transaction();
        ti = mdMgr.getTableInfo("students", tx);

        tcatfile = new RecordFile(ti, tx);
        for(i = 0; i < 7000; i++) {
            tcatfile.insert();
            tcatfile.setInt("id", i);
            tcatfile.setString("firstName", getRandomFirstName());
            tcatfile.setString("lastName", getRandomLastName());
        }
        tcatfile.close();
        singleStat = new JSONObject();
        singleStat.put("type", "insert7000");
        singleStat.put("data", SimpleDB.fileMgr().getAllBlockStats());
        data.put(singleStat);
        tx.commit();


        allStats.put(result);

        // cancello nuovamente le tracce del database
        deleteDatabase(dbName);
    }

    /**
     * Metodo per rimuovere le tracce del precedente database che potrebbero inquinare i benchmark.
     * Funziona solo su OS *nix
     * @param dbName
     * @throws Exception
     */
    private static void deleteDatabase(String dbName) throws Exception {
        Runtime.getRuntime().exec("rm -rf " + System.getProperty("user.home") + "/" + dbName).waitFor();
    }

    /**
     * Metodo che restituisce un nome casuale del famoso trio di comici.
     * @return
     */
    private static String getRandomFirstName() {
        String[] aRandomListOfFirstNames = { "Aldo", "Giovanni", "Giacomo" };
        int randomIndex = 0 + (int)(Math.random() * aRandomListOfFirstNames.length);
        return aRandomListOfFirstNames[randomIndex];
    }

    /**
     * Metodo che restituisce un cognome casuale del famoso trio di comici.
     * @return
     */
    private static String getRandomLastName() {
        String[] aRandomListOfFirstNames = { "Baglio", "Storti", "Poretti" };
        int randomIndex = 0 + (int)(Math.random() * aRandomListOfFirstNames.length);
        return aRandomListOfFirstNames[randomIndex];
    }

    private static String getRandomGender() {
        String[] genders = { "F", "M" };
        int randomIndex = 0 + (int)(Math.random() * 2);
        return genders[randomIndex];
    }

}