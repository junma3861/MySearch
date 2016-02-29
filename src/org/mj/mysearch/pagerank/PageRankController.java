/**
 * @author Jun
 */

package org.mj.mysearch.pagerank;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageRankController {
	
	private static final Logger logger = LoggerFactory.getLogger(PageRankController.class);
	
	
	public static void main(String[] args) {
		
		int maxIterNum = 100;
		double convergeThreshold = 1.0e-7;
		double defaultScoreValue = 0.01;
		double parameterD = 0.85;
		
		logger.info("Page Rank.");
		
		PageRank pageRank = new PageRank(maxIterNum, convergeThreshold, defaultScoreValue, parameterD);
		pageRank.run();
		
		logger.info("All complete.");
		
	}
	
	

}
