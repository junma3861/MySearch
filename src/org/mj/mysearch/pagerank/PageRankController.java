/**
 * @author Jun
 */

package org.mj.mysearch.pagerank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankController {
	
	private static final Logger logger = LoggerFactory.getLogger(PageRankController.class);
	
	
	public static void main(String[] args) {
		
		int maxIterNum = 1000;
		double convergeThreshold = 1.0e-5;
		double defaultScoreValue = 0;
		
		logger.info("Page Rank.");
		
		PageRank pageRank = new PageRank(maxIterNum, convergeThreshold, defaultScoreValue);
		pageRank.run();
		
		logger.info("All complete.");
		
	}
	
	

}
