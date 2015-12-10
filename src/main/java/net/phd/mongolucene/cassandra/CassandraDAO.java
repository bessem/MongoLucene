/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.cassandra;

import java.util.ArrayList;
import java.util.List;
import net.phd.mongolucene.utils.InputFileUtils;

/**
 *
 * @author aka
 */
public class CassandraDAO {

    public static final String DB_SRV = "localhost";
    public static final Integer DB_PORT = 27017;
    public static final String DEFAULT_COLLECTION_NAME = "users";
    private final String KEY_ID    = "id";
    private final String KEY_FNAME = "fname";
    private final String KEY_LNAME = "lname";
    private final String KEY_SPACE = "column_keyspace";
    private final String TAB_NAME  = "column";
    /**
     * Minimum number of fields in a line
     */
    private final Integer MIN_FIELDS_COUNT = 3;
    /**
     * The fields separator in the input file "|"
     */
    private final String FLD_SEPARATOR = "\\|";

    final CassandraConnector client = new CassandraConnector();

    /**
     *
     * @param inputFileURL -absolute path to input file
     */
    public void insertDataSet(String inputFileURL) {
        try {
            //Connection to Cassandra server
            client.connect(DB_SRV, DB_PORT);
            //start reading the input file. This step has been done separately to
            List<String> inputLines = InputFileUtils.loadDelimitedFile(inputFileURL);
            //transform lines to mongo objects
            List<Column> cols = transformRawDateToCleanCassColumn(inputLines, FLD_SEPARATOR);
            long before = System.currentTimeMillis();
            //the one by one insertion method
            cols.stream().forEach((col) -> {
                persistColumn(col);
            });
            long justAfter = System.currentTimeMillis();

            System.out.println("Inserted " + cols.size() + " items in " + (justAfter - before) + " ms.");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

    private List<Column> transformRawDateToCleanCassColumn(List<String> rawEntry, String separator) {
        List<Column> cleanOnes = null;
        try {
            if (rawEntry != null && rawEntry.size() > 0) {
                cleanOnes = new ArrayList<Column>();
                int irregularCount = 0, regularCount = 0;
                for (String line : rawEntry) {
                    String[] fields = line.split(separator);
                    if (fields != null && fields.length >= MIN_FIELDS_COUNT) {
                        regularCount++;
                        Column ctr = new Column();
                        ctr.id = fields[1];//ctr.setTel(fields[1]);//le num�ro
                        ctr.fname = fields[2];//setName0(fields[2]);
                        ctr.lname = fields[3];
                        cleanOnes.add(ctr);
                    } else {
                        System.out.println("Entry [" + line + "] is irregular.");
                        irregularCount++;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return cleanOnes;
    }

    public void persistColumn(Column col) {
        client.getSession().execute(
                "INSERT INTO " + KEY_SPACE + "." + TAB_NAME + " (" + KEY_ID + "," + KEY_FNAME + ", " + KEY_LNAME + ") VALUES (?, ?, ?)",
                col.id, col.fname, col.lname);
    }
}
