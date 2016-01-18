package org.mj.mysearch.pagerank;

import weka.core.matrix.Matrix;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;


public class PageRank {
	
	private double[][] matrixA;
	private double[] vectorPr;
	private Matrix wekaA, weakPr;
	
	
	
	/**
	 * constructor class
	 */
	public PageRank() {
		
	}
	
	
	/**
	 * Initialize matrix A and scores Pr
	 * 
	 */
	public void initialize() {
		
		loadMatrixAFromMongoDB();
	}
	
	
	/**
	 * 
	 */
	private void loadMatrixAFromMongoDB() {
		
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
