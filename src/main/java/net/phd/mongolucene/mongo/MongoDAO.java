/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.util.ArrayList;
import java.util.List;
import net.phd.mongolucene.utils.InputFileUtils;

/**
 *
 * @author aka
 */
public class MongoDAO {
 public static final String DB_SRV="localhost";
	public static final Integer DB_PORT =27017;
	public static final String DEFAULT_COLLECTION_NAME= "users";
	/**
	 * Minimum number of fields in a line
	 */
	private final Integer  MIN_FIELDS_COUNT = 3;
	/**
	 * The fields separator in the input file "|"
	 */
	private final String FLD_SEPARATOR = "\\|";
	/**
	 * 
	 * @param inputFileURL -absolute path to input file
	 */
	public void insertDataSet(String inputFileURL){
		try{
			MongoClient connection = new MongoClient( DB_SRV,DB_PORT);
			//the test database is called bench
			DB db = connection.getDB("bench");
			//the test collection is called users
			DBCollection collection = db.getCollection("users");
			//start reading the input file. This step has been done separately to
			List<String> inputLines = InputFileUtils.loadDelimitedFile(inputFileURL); 
			//transform lines to mongo objects
			List<DBObject> objects = transformRawDateToCleanMGObjects(inputLines,FLD_SEPARATOR);
			/* Either the one by one method
			for(DBObject obj: objects){
				collection.insert(obj);
			}
			 */
			/* Or the API provided bulk insert */
			long before = System.currentTimeMillis();
			collection.insert(objects);
			long justAfter = System.currentTimeMillis();
			connection.close();
			System.out.println("Inserted "+objects.size()+" items in "+(justAfter- before)+" ms.");
			
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	public List<DBObject> transformRawDateToCleanMGObjects(List<String> rawEntry, String separator){
		List<DBObject> cleanOnes= null;
		try{
			if(rawEntry != null && rawEntry.size()>0){
				cleanOnes = new ArrayList<DBObject>();
				int irregularCount =0, regularCount=0;;
				for(String line : rawEntry){
					String[] fields = line.split(separator);
					if(fields != null && fields.length>=MIN_FIELDS_COUNT){
						regularCount++;
						BasicDBObject ctr = new BasicDBObject();
							ctr.append("id",fields[1]);//ctr.setTel(fields[1]);//le num�ro
							ctr.append("fname",fields[2]);//setName0(fields[2]);
							ctr.append("lname",fields[3]);
							cleanOnes.add(ctr);
					}else{
						System.out.println("Entry ["+line+"] is irregular.");
						irregularCount ++;
					}
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return cleanOnes;
	}   
}
