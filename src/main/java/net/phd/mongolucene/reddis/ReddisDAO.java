/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.reddis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.phd.mongolucene.cassandra.Column;
import net.phd.mongolucene.utils.InputFileUtils;
import redis.clients.jedis.Jedis;

/**
 *
 * @author aka
 */
public class ReddisDAO {
    public static final String DB_SRV = "localhost";
    private final String KEY_ID    = "id";
    private final String KEY_FNAME = "fname";
    private final String KEY_LNAME = "lname";
    public Jedis jedis ; 
    /**
     * Minimum number of fields in a line
     */
    private final Integer MIN_FIELDS_COUNT = 3;
    /**
     * The fields separator in the input file "|"
     */
    private final String FLD_SEPARATOR = "\\|";
    public void insertDataSet(String inputFileURL) {
        List<Column> dataSet = new ArrayList<Column>();
        List<String> inputLines = InputFileUtils.loadDelimitedFile(inputFileURL);
        dataSet = transformRawDateToCleanRedisData(inputLines, FLD_SEPARATOR);
        try{
        jedis = new Jedis(DB_SRV);
        long before = System.currentTimeMillis();
            for(Column col : dataSet ){
                Map<String, String> userProperties = new HashMap<String, String>();
                userProperties.put(KEY_ID, col.id);
                userProperties.put(KEY_FNAME, col.fname);
                userProperties.put(KEY_LNAME, col.lname);
                jedis.hmset("User With ID ="+KEY_ID, userProperties);
            }
            long justAfter = System.currentTimeMillis();

            System.out.println("Inserted " + dataSet.size() + " items in " + (justAfter - before) + " ms.");
        }
        catch(Exception e){
            System.out.println("Unable to connect to server due to "+e.getMessage());
        }
        
    }
    private List<Column> transformRawDateToCleanRedisData(List<String> rawEntry, String separator) {
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
                        ctr.id = fields[1];
                        ctr.fname = fields[2];
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
}
