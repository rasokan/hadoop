package com.dnbtechmindz.codecontest.portfolioanalysis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PortfolioAnalysisDriver extends Configured implements Tool {

	private static final Log log = LogFactory
			.getLog(PortfolioAnalysisDriver.class);

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		log.info("Invoking PortfolioAnalysisDriver");

		Job job = new Job();

		job.setJarByClass(PortfolioAnalysisDriver.class);

		job.setJobName("PortfolioAnalysis");

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(PortfolioAnalysisMapper.class);

		job.setReducerClass(PortfolioAnalysisReducer.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new Configuration(),
				new PortfolioAnalysisDriver(), args);
		System.exit(res);
	}

}
