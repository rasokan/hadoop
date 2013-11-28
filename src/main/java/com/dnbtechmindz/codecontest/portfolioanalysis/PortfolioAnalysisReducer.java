package com.dnbtechmindz.codecontest.portfolioanalysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PortfolioAnalysisReducer extends Reducer<Text, Text, Text, Text> {

	private static final String FINANCIAL_RESULT_MEDIUM = "ABCD";

	private static final String FINANCIAL_RESULT_LOW = "EG";

	private static final String MEDIUM = "MEDIUM";

	private static final String HIGH = "HIGH";

	private static final String LOW = "LOW";

	private static final Log log = LogFactory
			.getLog(PortfolioAnalysisReducer.class);

	private static final String TAB_SPACE = "	";

	private static final String NEW_LINE = "\n";

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
	 * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<String, Integer> riskCategoryToDistributionCount = new HashMap<String, Integer>();
		Map<Integer, Integer> sicToRiskCategoryCount = new HashMap<Integer, Integer>();

		riskCategoryToDistributionCount.put(HIGH, 0);
		riskCategoryToDistributionCount.put(MEDIUM, 0);
		riskCategoryToDistributionCount.put(LOW, 0);

		String riskCategory = null;

		int size = 0;

		log.info("Executing reduce Function");

		context.write(new Text(""), new Text(displayHeader()));

		for (Text value : values) {

			String readLine = value.toString();

			String readLineValues[] = readLine.split(TAB_SPACE);

			String companyId = readLineValues[0];

			Integer sicId = Integer.parseInt(readLineValues[1]);

			String financialRating = readLineValues[2];

			Integer score1 = Integer.parseInt(readLineValues[3]);

			Integer score2 = Integer.parseInt(readLineValues[4]);

			Integer score3 = Integer.parseInt(readLineValues[5]);

			String financialRatingRisk = testFinancialRating(financialRating);

			log.info("Financial Rating Risk" + financialRatingRisk);

			String score1Risk = testScore1(score1);

			log.info("Score1 Risk" + score1Risk);

			String score2Risk = testScore2(score2);

			log.info("Score2 Risk" + score2Risk);

			String score3Risk = testScore3(score3);

			log.info("Score3 Risk" + score3Risk);

			riskCategory = testRiskCategory(financialRatingRisk, score1Risk,
					score2Risk, score3Risk);

			log.info("Reduce read Line:" + readLine);

			log.info("Reduce risk category:" + riskCategory);

			if (riskCategoryToDistributionCount.containsKey(riskCategory)) {

				Integer previousCount = riskCategoryToDistributionCount
						.get(riskCategory);

				Integer updatedCount = previousCount + 1;

				log.info("Updating Executing values:" + riskCategory
						+ updatedCount);

				riskCategoryToDistributionCount.put(riskCategory, updatedCount);

			} else {

				log.info("Adding new values:" + riskCategory);

				riskCategoryToDistributionCount.put(riskCategory, 1);
			}

			if (riskCategory.equalsIgnoreCase(HIGH)) {

				if (sicToRiskCategoryCount.containsKey(sicId)) {

					log.info("logging sicCount update:" + sicId);

					Integer previousCount = sicToRiskCategoryCount.get(sicId);

					Integer updatedCount = previousCount + 1;

					sicToRiskCategoryCount.put(sicId, updatedCount);

				} else {

					log.info("logging sicCount new:" + sicId);
					sicToRiskCategoryCount.put(sicId, 1);

				}
			}

			String resultToDisplay = display(companyId, sicId, financialRating,
					score1, score2, score3, financialRatingRisk, score1Risk,
					score2Risk, score3Risk, riskCategory);

			context.write(new Text(""), new Text(resultToDisplay));

			size = size + 1;
		}

		String distributionCountDisplay = display(
				riskCategoryToDistributionCount, size);

		context.write(new Text(""), new Text(distributionCountDisplay));

		String sicRiskCountDisplay = displayHighRisk(sicToRiskCategoryCount);

		context.write(new Text(""), new Text(sicRiskCountDisplay));
	}

	/**
	 * @param financialRating
	 * @return
	 */
	private String testFinancialRating(String financialRating) {

		String financialRatingResult = null;

		if (financialRating.charAt(0) == '1'
				|| financialRating.charAt(0) == '2'
				|| financialRating.charAt(0) == '3') {

			financialRatingResult = "HIGH";

		} else if (financialRating.charAt(0) == '4'
				|| financialRating.charAt(0) == '5') {

			Character financialRatingSecondChar = financialRating.charAt(1);

			char[] financialRatingMediumCharacters = FINANCIAL_RESULT_MEDIUM
					.toCharArray();

			for (int i = 0; i < financialRatingMediumCharacters.length; i++) {

				if ((Character) financialRatingMediumCharacters[i] == financialRatingSecondChar) {

					financialRatingResult = "MEDIUM";
				}
			}

			char[] financialRatingLowCharacters = FINANCIAL_RESULT_LOW
					.toCharArray();

			for (int i = 0; i < financialRatingLowCharacters.length; i++) {

				if ((Character) financialRatingLowCharacters[i] == financialRatingSecondChar) {

					financialRatingResult = "LOW";
				}
			}
		}

		return financialRatingResult;
	}

	/**
	 * @param financialRating
	 * @param score1
	 * @param score2
	 * @param score3
	 */
	private String testRiskCategory(String financialRating, String score1,
			String score2, String score3) {

		int highCount = 0;

		int mediumCount = 0;

		int lowCount = 0;

		String riskCategoryResult = null;

		List<String> riskCategoryList = new ArrayList<String>();

		riskCategoryList.add(financialRating);
		riskCategoryList.add(score1);
		riskCategoryList.add(score2);
		riskCategoryList.add(score3);

		for (String riskCategory : riskCategoryList) {

			if (riskCategory.equalsIgnoreCase(HIGH)) {

				highCount++;
			} else if (riskCategory.equalsIgnoreCase(MEDIUM)) {

				mediumCount++;
			} else {
				lowCount++;
			}
		}

		if (highCount >= 2) {

			riskCategoryResult = HIGH;

		}

		else if (highCount >= 1 && mediumCount >= 1) {

			riskCategoryResult = MEDIUM;
		}

		else if (mediumCount == 3 && lowCount == 1) {

			riskCategoryResult = MEDIUM;
		}

		else if (mediumCount == 4) {

			riskCategoryResult = MEDIUM;
		}

		else if (mediumCount == 2 && lowCount == 2) {

			riskCategoryResult = LOW;
		}

		else if (mediumCount == 1 && lowCount == 3) {

			riskCategoryResult = LOW;
		}

		else if (lowCount == 4) {

			riskCategoryResult = LOW;
		}

		return riskCategoryResult;
	}

	/**
	 * @param score2
	 * @return
	 */
	private String testScore2(Integer score2) {

		String score1Result = null;

		if (score2 >= 1 && score2 <= 25) {

			score1Result = "HIGH";
		} else if (score2 >= 26 && score2 <= 75) {

			score1Result = "MEDIUM";
		} else {
			score1Result = "LOW";
		}

		return score1Result;
	}

	/**
	 * @param score3
	 * @return
	 */
	private String testScore3(Integer score3) {

		String score1Result = null;

		if (score3 >= 1 && score3 <= 300) {

			score1Result = "HIGH";
		} else if (score3 >= 301 && score3 <= 750) {

			score1Result = "MEDIUM";
		} else {
			score1Result = "LOW";
		}

		return score1Result;
	}

	/**
	 * @param score1
	 * @return
	 */
	private String testScore1(Integer score1) {

		String score1Result = null;

		if (score1 >= 1 && score1 <= 4) {

			score1Result = "LOW";
		} else if (score1 >= 5 && score1 <= 7) {

			score1Result = "MEDIUM";
		} else {
			score1Result = "HIGH";
		}

		return score1Result;
	}

	/**
	 * @param companyId
	 * @param sicId
	 * @param financialRating
	 * @param score1
	 * @param score2
	 * @param score3
	 * @param financialRatingRisk
	 * @param score1Risk
	 * @param score2Risk
	 * @param score3Risk
	 * @param riskCategory
	 * @return
	 */
	private String display(String companyId, Integer sicId,
			String financialRating, Integer score1, Integer score2,
			Integer score3, String financialRatingRisk, String score1Risk,
			String score2Risk, String score3Risk, String riskCategory) {

		StringBuffer resultToWrite = new StringBuffer();

		resultToWrite.append(companyId).append(TAB_SPACE).append(sicId)
				.append(TAB_SPACE).append(financialRating).append(TAB_SPACE)
				.append(score1).append(TAB_SPACE).append(score2)
				.append(TAB_SPACE).append(score3).append(TAB_SPACE);

		StringBuffer commentsToWrite = new StringBuffer();

		commentsToWrite.append(financialRatingRisk).append(TAB_SPACE)
				.append(score1Risk).append(TAB_SPACE).append(score2Risk)
				.append(TAB_SPACE).append(score3Risk).append(TAB_SPACE)
				.append(riskCategory);

		resultToWrite.append(commentsToWrite.toString());

		return resultToWrite.toString();

	}

	/**
	 * @return
	 */
	public String displayHeader() {

		StringBuffer headerToWrite = new StringBuffer();

		headerToWrite.append("COMPANY Id").append(TAB_SPACE).append("SIC ID")
				.append(TAB_SPACE).append("RATING").append(TAB_SPACE)
				.append("SCORE1").append(TAB_SPACE).append("SCORE2")
				.append(TAB_SPACE).append("SCORE3").append(TAB_SPACE)
				.append("Rating Risk").append(TAB_SPACE).append("SCORE1 RISK")
				.append(TAB_SPACE).append("SCORE2 RISK").append(TAB_SPACE)
				.append("SCORE3 RISK").append(TAB_SPACE)
				.append("RISK CATEGORY");

		return headerToWrite.toString();
	}

	/**
	 * @param riskCategoryToDistributionCount
	 * @return
	 */
	private String display(
			Map<String, Integer> riskCategoryToDistributionCount, int size) {

		log.info("Logging size in reduce function:" + size);

		Float highCountPercentage = 0.0F;

		Float mediumCountPercentage = 0.0F;

		Float lowCountPercentage = 0.0F;

		int highCount = 0;

		int mediumCount = 0;

		int lowCount = 0;

		highCount = riskCategoryToDistributionCount.get(HIGH);

		mediumCount = riskCategoryToDistributionCount.get(MEDIUM);

		lowCount = riskCategoryToDistributionCount.get(LOW);

		StringBuffer disTributionCountToWrite = new StringBuffer();

		if (highCount >= 1)

			highCountPercentage = ((float) highCount / size) * 100;

		if (mediumCount >= 1)

			mediumCountPercentage = ((float) mediumCount / size) * 100;

		if (lowCount >= 1)

			lowCountPercentage = ((float) lowCount / size) * 100;

		disTributionCountToWrite.append("HighCount:").append(highCount)
				.append(TAB_SPACE).append("High Count Percentage:")
				.append(highCountPercentage).append(NEW_LINE)
				.append("MediumCount:").append(mediumCount).append(TAB_SPACE)
				.append("Medium Count Percentage:")
				.append(mediumCountPercentage).append(NEW_LINE)
				.append("LowCount:").append(lowCount).append(TAB_SPACE)
				.append("Low Count Percentage:").append(lowCountPercentage);

		return disTributionCountToWrite.toString();

	}

	/**
	 * @param sicToRiskCategoryCount
	 * @return
	 */
	private String displayHighRisk(Map<Integer, Integer> sicToRiskCategoryCount) {

		Integer maxCountValue = 0;

		Integer sicIdKey = 0;

		for (Map.Entry<Integer, Integer> sicToRiskCategoryCountValue : sicToRiskCategoryCount
				.entrySet()) {

			Integer sicId = sicToRiskCategoryCountValue.getKey();

			Integer sicIdCountValue = sicToRiskCategoryCountValue.getValue();

			if (sicIdCountValue >= maxCountValue) {

				maxCountValue = sicIdCountValue;

				sicIdKey = sicId;
			}

		}

		StringBuffer sicIdToDisplay = new StringBuffer();

		sicIdToDisplay.append("The Riskiest Sector id :").append(sicIdKey)
				.append(TAB_SPACE).append("with  number of Companies include:")
				.append(TAB_SPACE).append(maxCountValue);

		return sicIdToDisplay.toString();
	}
}
