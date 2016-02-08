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
	private Matrix wekaA, wekaPr, wekaPreviousPr, wekaDeg;
	private double defaulScoreValue;
	
	private static final String URL_DB_NAME = "OutgoingUrlDB", PRSCORE_DB_NAME = "PrScoreDB";
	
	private MongoClient mongoClient;
	private MongoDatabase outgoingUrlDB, prScoreDB;
	
	private int numOfPages;
	public int maxIterNum;
	public double convergeThreshold;
	
	/**
	 * constructor class
	 * @param defaultScoreValue 
	 */
	public PageRank(int maxIterNum, double convergeThreshold, double defaultScoreValue) {
		
		
		
		incomingUrls = new HashMap<>();
		outgoingDegs = new HashMap<>();
		pageIndices = new HashMap<>();
		
		this.defaulScoreValue = defaultScoreValue;
		this.convergeThreshold = convergeThreshold;
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
		logger.info("Initializing Page Rank settings ...");
		loadMatrixAFromMongoDB();
		constructMatrices();
		logger.info("Initialization complete,");
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
				if (outgoingDegs.containsKey(incomingUrlDocId)) {
					try {
						matrixA[pageIndices.get(docId)][pageIndices.get(incomingUrlDocId)] = 1.;
					} catch (Exception e) {
						logger.info("contains docId {}:{}", docId, pageIndices.containsKey(docId));
						logger.info("contains incomingUrlDocId {}:{}", incomingUrlDocId, pageIndices.containsKey(incomingUrlDocId));
					}
					
				}
			}
			matrixDeg[pageIndices.get(docId)][pageIndices.get(docId)] = 1. / outgoingDegs.get(docId);
			
		}
		
		wekaA = new Matrix(matrixA);
		wekaDeg = new Matrix(matrixDeg);
		wekaA = wekaA.times(wekaDeg);
		wekaPr = new Matrix(vectorPr, vectorPr.length);
		wekaPreviousPr = null;
		
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
		
		logger.info("Loading Matrix A from MongoDB ...");
		logger.info("Reading from database {}", URL_DB_NAME);
		
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
					logger.info("docId:{}", Integer.toString(docId));
					
					@SuppressWarnings("unchecked")
					ArrayList<Integer> outgoingDocIdList = (ArrayList<Integer>) document.get("link_docId");
					
					if (!outgoingDegs.containsKey(docId)) {
						outgoingDegs.put(docId, outgoingDocIdList.size());
					} else {
						logger.warn("Repeated docId {} found in {}", docId, URL_DB_NAME);
						outgoingDegs.put(docId, outgoingDocIdList.size());
					}
					

					for (int item : outgoingDocIdList) {
						
						int outgoingDocId = item;
						
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
		logger.info("Shutting down MongoDB.");
		
		try {
			
			if (mongoClient != null) {
				mongoClient.close();
			}

		} catch (Exception e) {
			logger.error("Error while shutting down MongoDB Client.");
			e.printStackTrace();
		}
		logger.info("MongoDB is shut down.");
	}
	
	
	
	/**
	 * core function running PageRank
	 */
	protected void iterRun() {
		
		int iter = 0;
		double dist;
		
		logger.info("Start page ranking iterations ...");
		
		while ((dist = getConvergedDistance()) > convergeThreshold && iter++ < maxIterNum) {
			logger.info("Iter {}: distance: {}", iter, dist);
			wekaPreviousPr = wekaPr;
			wekaPr = wekaA.times(wekaPr);
		}
		
		logger.info("Iteration completed.");
	}
	

	
	/**
	 * Return the converged distance.
	 * @return
	 */
	protected double getConvergedDistance() {
		
		return wekaPreviousPr.minus(wekaPr).norm2();	
	}
	
	
	/**
	 * Save results to MongoDB
	 */
	protected void savePr() {
		
		logger.info("Saving PageRank score to MongoDB {} ...", PRSCORE_DB_NAME);
		
		try {
			
			mongoClient = new MongoClient();
			prScoreDB = mongoClient.getDatabase(PRSCORE_DB_NAME);
			logger.info("Successfully opened database {}.", PRSCORE_DB_NAME);
			
			for (int docId : pageIndices.keySet()) {
				prScoreDB.getCollection(PRSCORE_DB_NAME).insertOne(new Document().append("doc_id", docId)
						.append("pr_score", wekaPr.get(pageIndices.get(docId), 0) ));
			}
			
			
		} catch (Exception dbe) {
			logger.error("Error while openining index or outgoingUrlDB database.");
			dbe.printStackTrace();			
		} finally {
			shutDownDB();
		}
		
		logger.info("Saving PageRank score completed.");
		
	}
	
	
	/**
	 * call this function to run
	 */
	public void run() {
		initialize();
		iterRun();
		savePr();
	}

}
