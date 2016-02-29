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
	private double[] vectorPr, vectorD;
	private Matrix wekaA, wekaPr, wekaPreviousPr, wekaDeg, wekaD;
	private double defaulScoreValue;
	
	private static final String URL_DB_NAME = "OutgoingUrlDB", PRSCORE_DB_NAME = "PrScoreDB";
	
	private MongoClient mongoClient;
	private MongoDatabase outgoingUrlDB, prScoreDB;
	
	private int numOfPages;
	public int maxIterNum;
	public double convergeThreshold, parameterD;
	
	/**
	 * constructor class
	 * @param defaultScoreValue 
	 */
	public PageRank(int maxIterNum, double convergeThreshold, double defaultScoreValue, double parameterD) {
		
		
		
		incomingUrls = new HashMap<>();
		outgoingDegs = new HashMap<>();
		pageIndices = new HashMap<>();
		
		this.defaulScoreValue = defaultScoreValue;
		this.convergeThreshold = convergeThreshold;
		this.maxIterNum = maxIterNum;
		this.parameterD = parameterD;
		
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
		
		
		logger.info("Constucting Matrics ...");
		
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
		vectorD = new double[numOfPages];
		Arrays.fill(vectorPr, new Double(defaulScoreValue));
		
		for (int docId : outgoingDegs.keySet()) {
			if (!incomingUrls.containsKey(docId)) continue;
			for (int incomingUrlDocId : incomingUrls.get(docId)) {
				if (pageIndices.containsKey(incomingUrlDocId)) {
					try {
						matrixA[pageIndices.get(docId)][pageIndices.get(incomingUrlDocId)] = 1.;
					} catch (Exception e) {
						logger.info("contains docId {}:{}", docId, pageIndices.containsKey(docId));
						logger.info("contains incomingUrlDocId {}:{}", incomingUrlDocId, pageIndices.containsKey(incomingUrlDocId));
					}
					
				}
			}
			matrixDeg[pageIndices.get(docId)][pageIndices.get(docId)] = outgoingDegs.get(docId) == 0 ? 0 : 1. / outgoingDegs.get(docId);
			
		}
		
		
		for (int i = 0; i < numOfPages; i++) {
			vectorD[i] = 1. / numOfPages;
		}
		
		wekaA = new Matrix(matrixA);
		logger.info("wekaA norm: {}, wekaA dim: {},{}, 1st element:{}", wekaA.normF(), wekaA.getRowDimension(), wekaA.getColumnDimension(), wekaA.get(0, 0));
		wekaDeg = new Matrix(matrixDeg);
		logger.info("wekaDeg norm: {}, wekaDeg dim: {}, {}, 1st element:{}", wekaDeg.normF(), wekaDeg.getRowDimension(), wekaDeg.getColumnDimension(), wekaDeg.get(0, 0));
		wekaA = wekaA.times(wekaDeg);
		logger.info("wekaA norm: {}, wekaA dim: {},{}, 1st element:{}", wekaA.normF(), wekaA.getRowDimension(), wekaA.getColumnDimension(), wekaA.get(0, 0));
		logger.info("wekaA norminf: {}, wekaA norm1: {}", wekaA.normInf(), wekaA.norm1());
		

		
		// Normalization
		for (int i = 0; i < numOfPages; i++) {
			int sum = 0;
			for (int j = 0; j < numOfPages; j++) {
				sum += wekaA.get(i, j);
			}
			for (int j = 0; j < numOfPages; j++) {
				if (sum == 0) {
					wekaA.set(i, j, 0);
				} else {
					wekaA.set(i, j, (double) wekaA.get(i, j) / sum);
				}
				
			}
		}
		
		
		// value check
//		for (int i = 0; i < numOfPages; i++) {
//		for (int j = 0; j < numOfPages; j++) {
//			if (wekaA.get(i, j) == 1) {
//				logger.info("{}, {} in wekaA == 1", i, j);
//			}
//		}
//	}
		
		wekaPr = new Matrix(vectorPr, vectorPr.length);
		wekaD = new Matrix(vectorD, vectorD.length);
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
					
					@SuppressWarnings("unchecked")
					ArrayList<Integer> outgoingDocIdList = (ArrayList<Integer>) document.get("link_docId");
					logger.info("docId:{}, size:{}", Integer.toString(docId), outgoingDocIdList.size());
					
					if (!outgoingDegs.containsKey(docId)) {
						outgoingDegs.put(docId, Math.max(1, outgoingDocIdList.size()));
					} else {
						logger.warn("Repeated docId {} found in {}", docId, URL_DB_NAME);
						outgoingDegs.put(docId, Math.max(1, outgoingDocIdList.size()));
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
		double dist = 10 * convergeThreshold;
		
		logger.info("Start page ranking iterations ...");
		
		wekaD = wekaD.times((1 - parameterD));
		while (dist > convergeThreshold && iter++ < maxIterNum) {
			
			//logger.info("element in welaPr: {}");
			wekaPreviousPr = wekaPr.copy();
			logger.info("wekaA norm: {}", wekaA.normF());
			wekaPr = wekaA.times(wekaPr).times(parameterD).plus(wekaD);
			logger.info("Norm in the middle: {}", wekaPr.normF());
			//wekaPr = wekaPr.times(parameterD).plus(wekaD);
			logger.info("wekaPreviousPr Norm: {}", wekaPreviousPr.normF());
			logger.info("WekaPr Norm: {}", wekaPr.normF());
			dist = getConvergedDistance();
			logger.info("Iter :{} distance: {}", iter, dist);
		}
		
		logger.info("distance: {}", dist);
		logger.info("Iteration completed.");
	}
	

	
	/**
	 * Return the converged distance.
	 * @return
	 */
	protected double getConvergedDistance() {
		return wekaPreviousPr.minus(wekaPr).normF();	
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
