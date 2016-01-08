/**
 * 
 * @author Jun
 */

package org.mj.mysearch.revindexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RevIndexerController {

	private static final Logger logger = LoggerFactory.getLogger(RevIndexerController.class);
	
	public static void main(String[] args) {
		
		logger.info("Running reverse indexing...");
		
		RevIndexer revIndexer = new RevIndexer();
		revIndexer.initialize();
		revIndexer.start();
		
		logger.info("All complete.");
		
	}
}
