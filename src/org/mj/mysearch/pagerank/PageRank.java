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
	
	private HashMap<Integer, List<Integer>> incomingUrls;
	private HashMap<Integer, Integer> outgoingDegs;
	private HashMap<Integer, Integer> pageIndices;
	
	private double[][] matrixA;
	private double[][] matrixDeg;
	private double[] vectorPr;
	private Matrix wekaA, wekaPr, wekaDeg;
	private double defaulScoreValue;
	
	private static final String URL_DB_NAME = "OutgoingUrlDB";
	
	private MongoClient mongoClient;
	private MongoDatabase outgoingUrlDB;
	
	private int numOfPages;
	public int maxIterNum;
	
	/**
	 * constructor class
	 * @param defaultScoreValue 
	 */
	public PageRank(int maxIterNum, double defaultScoreValue) {
		
		
		
		incomingUrls = new HashMap<>();
		outgoingDegs = new HashMap<>();
		pageIndices = new HashMap<>();
		
		this.defaulScoreValue = defaultScoreValue;
		this.maxIterNum = maxIterNum;
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
		constructMatrices();
	}
	
	
	/**
	 * Use the constructed HashMap: incomingUrls, outgoingDegs
	 * construct the weka Matrices: wekaA, wekaPc
	 * 
	 * Steps:
	 * 1. construct page indices: HashMap docId=>int, position in the matrix row/col
	 * 2. construct matrixA, vectorPr
	 * 3. construct wekaA, wekaP from matrixA and vectorPr
	 */
	private void constructMatrices() {
		
		numOfPages = outgoingDegs.keySet().size();
		
		int currentIndex = 0;
		for (int docId : outgoingDegs.keySet()) {
			if (!pageIndices.containsKey(docId)) {
				pageIndices.put(docId, currentIndex++);
			}
		}
		
		assert (currentIndex + 1) == numOfPages;
		
		matrixA = new double[numOfPages][numOfPages];
		matrixDeg = new double[numOfPages][numOfPages];
		vectorPr = new double[numOfPages];
		Arrays.fill(vectorPr, new Double(defaulScoreValue));
		
		for (int docId : incomingUrls.keySet()) {
			for (int incomingUrlDocId : incomingUrls.get(docId)) {
				if (outgoingDegs.keySet().contains(incomingUrlDocId)) {
					matrixA[pageIndices.get(docId)][pageIndices.get(incomingUrlDocId)] = 1.;
				}
			}
			matrixDeg[pageIndices.get(docId)][pageIndices.get(docId)] = 1. / outgoingDegs.get(docId);
			
		}
		
		wekaA = new Matrix(matrixA);
		wekaDeg = new Matrix(matrixDeg);
		wekaA = wekaA.times(wekaDeg);
		wekaPr = new Matrix(vectorPr, vectorPr.length);
		
		matrixA = null;
		matrixDeg = null;
		wekaDeg = null;
		vectorPr = null;
		
		
	}
	
	
	
	/**
	 * For each docId : outgoing LinkedDocIds pair, reverse it as docId : incoming LinkedDocIds, 
	 * save in a HashMap: incomingUrls
	 * Also save the outgoing degree in a HashMap: outgoingDegs
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
					
					int docId = document.getInteger("doc_id");
					logger.info(Integer.toString(docId));
					
					@SuppressWarnings("unchecked")
					ArrayList<Document> outgoingDocIdList = (ArrayList<Document>) document.get("link_docId");
					
					if (!outgoingDegs.containsKey(docId)) {
						outgoingDegs.put(docId, outgoingDocIdList.size());
					} else {
						logger.warn("Repeated docId {} found in {}", docId, URL_DB_NAME);
						outgoingDegs.put(docId, outgoingDocIdList.size());
					}
					

					for (Document item : outgoingDocIdList) {
						
						int outgoingDocId = item.getInteger("link_docId");
						
						if (!incomingUrls.containsKey(outgoingDocId)) {
							incomingUrls.put(outgoingDocId, new ArrayList<Integer>(Arrays.asList(docId)));
						} else {
							incomingUrls.get(outgoingDocId).add(docId);
						}
						
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
		
		int iter = 0;
		
		while (!isConverged() && iter < maxIterNum) {
			
		}
	}
	
	/**
	 * Test if the convergence condition is met.
	 * @return
	 */
	protected boolean isConverged() {
		
		return false;
	}

}
