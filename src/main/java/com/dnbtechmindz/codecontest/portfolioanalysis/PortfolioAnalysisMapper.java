package com.dnbtechmindz.codecontest.portfolioanalysis;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PortfolioAnalysisMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private static final Log log = LogFactory
			.getLog(PortfolioAnalysisMapper.class);

	private static final String TAB_SPACE = "	";

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		log.info("Executing map function");

		String lineRead = value.toString();

		String lineReadValues[] = lineRead.split(TAB_SPACE);

		String companyId = lineReadValues[0];

		Long sicId = Long.parseLong(lineReadValues[1]);

		String financialRating = lineReadValues[2];

		Integer score1 = Integer.parseInt(lineReadValues[3]);

		Integer score2 = Integer.parseInt(lineReadValues[4]);

		Integer score3 = Integer.parseInt(lineReadValues[5]);

		StringBuffer resultToWrite = new StringBuffer();

		resultToWrite.append(companyId).append(TAB_SPACE).append(sicId)
				.append(TAB_SPACE).append(financialRating).append(TAB_SPACE)
				.append(score1).append(TAB_SPACE).append(score2)
				.append(TAB_SPACE).append(score3);

		context.write(new Text(""), new Text(resultToWrite.toString()));

	}

}
