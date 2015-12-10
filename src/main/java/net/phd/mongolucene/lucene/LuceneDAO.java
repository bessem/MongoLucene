/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.phd.mongolucene.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.phd.mongolucene.utils.InputFileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author aka
 */
public class LuceneDAO {
    	/**
	 * Minimum number of fields in a line
	 */
	private final Integer  MIN_FIELDS_COUNT = 3;
	/**
	 * The fields separator in the input file "|"
	 */
	private final String FLD_SEPARATOR = "\\|";
	/**
	 * absolute path to index repository
	 */
	private final String INDEX_DIRECTORY= "D:\\fadoua\\idx\\";
	
	public void indexInputFileAsObjects(String inputFile) throws IOException{
		//start reading the input file. This step has been done separately to
		Directory dir=FSDirectory.open(new File(INDEX_DIRECTORY));
		StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_4_10_4);
		List<String> inputLines = InputFileUtils.loadDelimitedFile(inputFile);
                List<Document> documents = transformRawDateToCleanLuceneDocuments(inputLines,FLD_SEPARATOR);
                IndexWriterConfig iwc = new IndexWriterConfig(Version.
                                LUCENE_4_10_4,analyzer);
                iwc.setOpenMode(OpenMode.CREATE);
		try {   
			IndexWriter iw = new IndexWriter(dir,iwc);
                        long before = System.currentTimeMillis();
                        for(Document doc : documents){
                            iw.addDocument(doc);
                        }
                        long justAfter = System.currentTimeMillis();
                           System.out.println("Inserted "+documents.size()+" items in "+(justAfter- before)+" ms.");
		} catch (Exception e) {
			   System.out.println(e.getMessage());
		}
	}
        public List<Document> transformRawDateToCleanLuceneDocuments(List<String> rawEntry, String separator){
		List<Document> cleanOnes= null;
		try{
			if(rawEntry != null && rawEntry.size()>0){
				cleanOnes = new ArrayList<Document>();
				int irregularCount =0, regularCount=0;;
				for(String line : rawEntry){
					String[] fields = line.split(separator);
					if(fields != null && fields.length>=MIN_FIELDS_COUNT){
						regularCount++;
						Document ctr = new Document();
							ctr.add(new Field("id"   ,fields[1],Field.Store.YES,Field.Index.NOT_ANALYZED));
                                                        ctr.add(new Field("fname",fields[2],Field.Store.YES,Field.Index.NOT_ANALYZED));//setName0(fields[2]);
							ctr.add(new Field("lname",fields[3],Field.Store.YES,Field.Index.NOT_ANALYZED));
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
