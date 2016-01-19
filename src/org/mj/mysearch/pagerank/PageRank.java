package org.mj.mysearch.pagerank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import weka.core.matrix.Matrix;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.mj.mysearch.webcrawler.TextCrawlerMongoDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PageRank {
	
	private static final Logger logger = LoggerFactory.getLogger(PageRank.class);
	
	HashMap<String, List<String>> incomingUrls = new HashMap<>();
	HashMap<String, Integer> outgoingDegs = new HashMap<>();
	
	private double[][] matrixA;
	private double[] vectorPr;
	private Matrix wekaA, weakPr;
	
	private static final String URL_DB_NAME = "OutgoingUrlDB";
	
	private MongoClient mongoClient;
	private MongoDatabase outgoingUrlDB;
	/**
	 * constructor class
	 */
	public PageRank() {
		
	}
	
	
	/**
	 * Initialize matrix A and scores Pr
	 * 
	 * Construct matrixA N x N (N = number of pages)
	 * 1. matrixAdj[i][j] = 1 indicates page j points to page i, 0 otherwise.
	 * 2. construct diagonal matrix matrixDeg[i][i] = 1/(number of pages leaving page i)
	 * 3. matrixA = matrixAdj x matrixDeg
	 */
	public void initialize() {
		
		
		loadMatrixAFromMongoDB();
	}
	
	
	/**
	 * For each docId : outgoing LinkedDocIds pair, reverse it as docId : incoming LinkedDocIds, save in a HashMap
	 * Also save the outgoing degree in a HashMap.
	 */
	private void loadMatrixAFromMongoDB() {
		
		logger.info("Opening database {}");
		try {
			mongoClient = new MongoClient();
			outgoingUrlDB = mongoClient.getDatabase(URL_DB_NAME);
			logger.info("Successfully opened database {}.", URL_DB_NAME);

			
			FindIterable<Document> iterable = outgoingUrlDB.getCollection("DocId_LinkDocId").find();
			iterable.noCursorTimeout(true);
			
			iterable.forEach(new Block<Document>() {
				
				@Override
				public void apply(final Document document) {
					
					String docId = document.getInteger("doc_id").toString();
					logger.info(docId);
					
					@SuppressWarnings("unchecked")
					ArrayList<Document> outgoingDocIdList = (ArrayList<Document>) document.get("link_docId");
					
					
					for (Document item : outgoingDocIdList) {
						
						String word = item.keySet().iterator().next();
						
						if (!outgoingDegs.containsKey(word)) {
							outgoingDegs.put(word, 0);
						} else {
							logger.warn("Repeated docId {} found in {}", word, URL_DB_NAME);
						}
						
//						if (!isWordInRevIndexDB(word)) {
//							revIndexDB.getCollection("Word_DocId").insertOne(new Document().append("word", word)
//									.append("word_count_in_docId", Arrays.asList()));
//						}
//						
//						revIndexDB.getCollection("Word_DocId").updateOne(new Document("word", word), 
//								new Document("$push", new Document(docId, item.getInteger(word))));
						
					}
					
				}
			});
			
			
		} catch (Exception dbe) {
			logger.error("Error while openining index or outgoingUrlDB database.");
			dbe.printStackTrace();			
		} finally {
			shutDownDB();
		}
	}
	
	
	/**
	 *  Shut down database.
	 */
	private void shutDownDB() {
		try {
			
			if (mongoClient != null) {
				mongoClient.close();
			}

		} catch (Exception e) {
			logger.error("Error while shutting down MongoDB Client.");
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * core finction running PageRank
	 */
	public void run() {
		
	}
	
	/**
	 * Test if the convergence condition is met.
	 * @return
	 */
	protected boolean isConverged() {
		
		return false;
	}

}
