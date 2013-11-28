package com.dnbtechmindz.codecontest.portfolioanalysis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

/**
 * @author ashok
 * 
 */
public class UtilTest {

	private static final String FINANCIAL_RESULT_MEDIUM = "ABCD";

	private static final String FINANCIAL_RESULT_LOW = "EG";

	private static final String MEDIUM = "MEDIUM";

	private static final String HIGH = "HIGH";

	private static final String LOW = "LOW";

	@Test
	public void testFinancialRatingHigh() {

		List<String> financialRatingList = new ArrayList<String>();

		financialRatingList.add("3A1");

		financialRatingList.add("1G4");
		
		financialRatingList.add("1A1");

		financialRatingList.add("2B5");

		for (String financialRating : financialRatingList) {

			Assert.assertEquals("HIGH", testFinancialRating(financialRating));
		}
	}

	@Test
	public void testFinancialRatingMedium() {

		List<String> financialRatingList = new ArrayList<String>();

		financialRatingList.add("4A3");

		financialRatingList.add("5D4");

		for (String financialRating : financialRatingList) {

			Assert.assertEquals("MEDIUM", testFinancialRating(financialRating));
		}

	}

	@Test
	public void testFinancialRatingLOW() {

		List<String> financialRatingList = new ArrayList<String>();

		financialRatingList.add("4E4");

		financialRatingList.add("5G2");

		for (String financialRating : financialRatingList) {

			Assert.assertEquals("LOW", testFinancialRating(financialRating));
		}

	}

	@Test
	public void testScore1HIGH() {

		for (int i = 8; i <= 10; i++) {

			String resultHigh = testScore1(i);

			Assert.assertEquals("HIGH", resultHigh);
		}

	}

	@Test
	public void testScore1Medium() {

		for (int i = 5; i <= 7; i++) {

			String resultHigh = testScore1(i);

			Assert.assertEquals("MEDIUM", resultHigh);
		}

	}

	@Test
	public void testScore1LOW() {

		for (int i = 1; i <= 4; i++) {

			String resultHigh = testScore1(i);

			Assert.assertEquals("LOW", resultHigh);
		}

	}

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

	@Test
	public void testScore2HIGH() {

		for (int i = 1; i <= 25; i++) {

			String resultHigh = testScore2(i);

			Assert.assertEquals("HIGH", resultHigh);
		}

	}

	@Test
	public void testScore2Medium() {

		for (int i = 26; i <= 75; i++) {

			String resultHigh = testScore2(i);

			Assert.assertEquals("MEDIUM", resultHigh);
		}

	}

	@Test
	public void testScore2LOW() {

		for (int i = 76; i <= 100; i++) {

			String resultHigh = testScore2(i);

			Assert.assertEquals("LOW", resultHigh);
		}

	}

	@Test
	public void testScore3HIGH() {

		for (int i = 1; i <= 300; i++) {

			String resultHigh = testScore3(i);

			Assert.assertEquals("HIGH", resultHigh);
		}

	}

	@Test
	public void testScore3Medium() {

		for (int i = 301; i <= 750; i++) {

			String resultHigh = testScore3(i);

			Assert.assertEquals("MEDIUM", resultHigh);
		}

	}

	@Test
	public void testScore3LOW() {

		for (int i = 751; i <= 1000; i++) {

			String resultHigh = testScore3(i);

			Assert.assertEquals("LOW", resultHigh);
		}

	}

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

	@Test
	public void testRiskCategoryHIGH() {

		String financialRating = testFinancialRating("3A1");

		String score1 = testScore1(8);

		String score2 = testScore2(10);

		String score3 = testScore3(200);

		Assert.assertEquals(HIGH,
				testRiskCategory(financialRating, score1, score2, score3));

	}

	@Test
	public void testRiskCategory1HIGH2MEDIUM() {

		String financialRating = testFinancialRating("3A1");

		String score1 = testScore1(5);

		String score2 = testScore2(50);

		String score3 = testScore3(350);

		Assert.assertEquals(MEDIUM,
				testRiskCategory(financialRating, score1, score2, score3));

	}

	@Test
	public void testRiskCategory1HIGH1MEDIUM() {

		String financialRating = testFinancialRating("3A1");

		String score1 = testScore1(5);

		String score2 = testScore2(90);

		String score3 = testScore3(760);

		Assert.assertEquals(MEDIUM,
				testRiskCategory(financialRating, score1, score2, score3));
	}

	@Test
	public void testRiskCategory2MEDIUM2LOW() {

		String financialRating = testFinancialRating("4A3");

		String score1 = testScore1(5);

		String score2 = testScore2(90);

		String score3 = testScore3(760);

		Assert.assertEquals(LOW,
				testRiskCategory(financialRating, score1, score2, score3));
	}

	@Test
	public void testRiskCategory3MEDIUM1LOW() {

		String financialRating = testFinancialRating("4A3");

		String score1 = testScore1(5);

		String score2 = testScore2(70);

		String score3 = testScore3(760);

		Assert.assertEquals(MEDIUM,
				testRiskCategory(financialRating, score1, score2, score3));
	}

	@Test
	public void testRiskCategory4MEDIUM() {

		String financialRating = testFinancialRating("4A3");

		String score1 = testScore1(5);

		String score2 = testScore2(70);

		String score3 = testScore3(500);

		Assert.assertEquals(MEDIUM,
				testRiskCategory(financialRating, score1, score2, score3));
	}

	@Test
	public void testRiskCategory1MEDIUM3LOW() {

		String financialRating = testFinancialRating("4A3");

		String score1 = testScore1(3);

		String score2 = testScore2(80);

		String score3 = testScore3(800);

		Assert.assertEquals(LOW,
				testRiskCategory(financialRating, score1, score2, score3));
	}

	@Test
	public void testRiskCategory4LOW() {

		String financialRating = testFinancialRating("4E4");

		String score1 = testScore1(3);

		String score2 = testScore2(80);

		String score3 = testScore3(800);

		Assert.assertEquals(LOW,
				testRiskCategory(financialRating, score1, score2, score3));
		
		
	}
	
	
	@Test
	public void testRiskCategory4HIGH() {

		String financialRating = testFinancialRating("5G2");

		String score1 = testScore1(8);

		String score2 = testScore2(40);

		String score3 = testScore3(50);
		
		Assert.assertEquals(HIGH,
				testRiskCategory(financialRating, score1, score2, score3));
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

	@Test
	public void testMap() {

		Map<Integer, Integer> valueMap = new HashMap<Integer, Integer>();

		valueMap.put(1, 100);

		valueMap.put(2, 100);

		processMap(valueMap);
	}

	private void processMap(Map<Integer, Integer> valueMap) {

		System.out.println(valueMap.size());

	}

	@Test
	public void testPercentage() {

		int highCount = 2;

		int size = 3;

		float highCountPercentage = 0.0F;

		highCountPercentage = ((float) highCount / size) * 100;

		System.out.println(highCountPercentage);

	}

}
