/**
 * 
 * @author Jun
 */

package org.mj.mysearch.revindexer;


import java.util.ArrayList;
import java.util.Arrays;




import com.mongodb.MongoClient;
import com.mongodb.BasicDBList;
import com.mongodb.Block;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.FindIterable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RevIndexer {
	
	private static final Logger logger = LoggerFactory.getLogger(RevIndexer.class);
	
	private static final String INDEX_DB_NAME = "WebCrawlerIndexDB";
	private static final String REV_INDEX_DB = "RevIndexDB";
	
	private MongoClient mongoClient;
	private MongoDatabase indexDB, revIndexDB;
	
	protected final Object mutex = new Object();
	

	/**
	 * class constructor
	 */
	public RevIndexer() {
		
		
	}
	
	
	/**
	 * Initialize the databases
	 */
	public void initialize() {
		
		try {
			
			mongoClient = new MongoClient();
			indexDB = mongoClient.getDatabase(INDEX_DB_NAME);
			mongoClient.dropDatabase(REV_INDEX_DB);
			revIndexDB = mongoClient.getDatabase(REV_INDEX_DB);
			
			
			logger.info("Successfully initialized {} and {}.", INDEX_DB_NAME, REV_INDEX_DB);
			//txnIndexDB = indexDBEnv.beginTransaction(null, null);
			//txnOutgoingUrlDB = outgoingDBEnv.beginTransaction(null, null);
			
		} catch (Exception dbe) {
			shutDown();
			logger.error("Error while openining index or outgoingUrlDB database.");
			dbe.printStackTrace();			
			
		}
		
	}
	
	
	/**
	 * 
	 */
	protected void shutDown() {
		
		logger.info("Shutting down databases.");
		
		try {
			mongoClient.close();
			
		} catch (Exception e) {
			logger.error("Error while shutting down MongoDB Client.");
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Start
	 */
	public void start() {
		
		try {
			
			// For each entry
			FindIterable<Document> iterable = indexDB.getCollection("DocId_WordCount").find();
			
			iterable.noCursorTimeout(true);
			
			iterable.forEach(new Block<Document>() {
				
				@Override
				public void apply(final Document document) {
					
					String docId = document.getInteger("doc_id").toString();
					logger.info(docId);
					
					@SuppressWarnings("unchecked")
					ArrayList<Document> wordCountList = (ArrayList<Document>) document.get("word_count");
					
					
					for (Document item : wordCountList) {
						
						String word = item.keySet().iterator().next();
						
						if (!isWordInRevIndexDB(word)) {
							revIndexDB.getCollection("Word_DocId").insertOne(new Document().append("word", word)
									.append("word_count_in_docId", Arrays.asList()));
						}
						
						revIndexDB.getCollection("Word_DocId").updateOne(new Document("word", word), 
								new Document("$push", new Document(docId, item.getInteger(word))));
						
					}
					
				}
			});
			
			
			
		} catch (Exception e) {
			
			logger.error("Error from function start()");
			e.printStackTrace();
			shutDown();
		}
		
	}
	
	
	/**
	 * 
	 * Test if the document with word is already created
	 * @param word
	 * @return
	 */
	private boolean isWordInRevIndexDB(String word) {
		return revIndexDB.getCollection("Word_DocId").find(new Document("word", word)).limit(1).iterator().hasNext();
	}

}
