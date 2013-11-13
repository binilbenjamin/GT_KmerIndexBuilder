package edu.arizona.cs.gt.kmer;

import java.io.File;
import java.io.FileOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Shell;

public class GTToolInvokerMapper extends Mapper<NullWritable, BytesWritable, Text, Text> {

	protected void map(NullWritable key, BytesWritable value,
			org.apache.hadoop.mapreduce.Mapper<NullWritable, BytesWritable, Text, Text>.Context context)
			throws java.io.IOException, InterruptedException {

		FileSplit split = (FileSplit) context.getInputSplit();
		String fastaName = split.getPath().getName().split("\\.")[0];

		/*
		 * Create a temp directory to store the input files for GT tool
		 */
		File workDir = new File(System.getProperty("java.io.tmpdir") + File.separator + fastaName);
		if (!workDir.exists()) {
			workDir.mkdirs();
		}

		/*
		 * Write the input split into the temp directory
		 */
		File inputFastaFile = new File(workDir, split.getPath().getName());
		FileOutputStream fos = new FileOutputStream(inputFastaFile);
		fos.write(value.getBytes());
		fos.close();

		/*
		 * Invoke GT tool
		 */
		String gtToolLocation = context.getConfiguration().get("gt.loc");
		String merSize = context.getConfiguration().get("gt.tallymer.mersize", "20");
		String parts = context.getConfiguration().get("gt.suffixerator.parts", "4");
		String suffixeratorIndexName = context.getConfiguration().get("gt.suffixerator.indexname", "reads");
		String tallymerIndexName = context.getConfiguration().get("gt.tallymer.indexname", "tyr-reads");

		System.out.println("inputFastaFile.getAbsolutePath() = "+inputFastaFile.getAbsolutePath());
		System.out.println(Shell.execCommand(gtToolLocation, "suffixerator", "-dna", "-pl", "-tis", "-suf", "-lcp",
				"-parts", parts, "-db", inputFastaFile.getAbsolutePath(), "-indexname", workDir.getAbsolutePath()
						+ File.separator + suffixeratorIndexName));

		
		System.out.println("REACHED HERE");
		System.out.println(Shell.execCommand(gtToolLocation, "tallymer", "mkindex", "-mersize", merSize, "-minocc",
				"1", "-indexname", workDir.getAbsolutePath() + File.separator + tallymerIndexName, "-counts", "-pl",
				"-esa", workDir.getAbsolutePath() + File.separator + suffixeratorIndexName));

		/*
		 * Delete the temporary files and upload the GT output back to HDFS
		 */
		inputFastaFile.delete();

		FileSystem dfsFileSystem = FileSystem.get(context.getConfiguration());
		Path outputPath = FileOutputFormat.getOutputPath(context);

		// delete the target dir for this mapper if it exists (usually not
		// required, but have seen one instance where a mapper was restarted and
		// the dir existed from the previous run, causing the new mapper to
		// go into loop
		Path fastaOutputPath = new Path(outputPath, fastaName);
		if (dfsFileSystem.exists(fastaOutputPath)) {
			dfsFileSystem.delete(fastaOutputPath, true);
		}

		FileUtil.copy(workDir, dfsFileSystem, outputPath, false, context.getConfiguration());

		FileUtils.deleteDirectory(workDir);

		context.write(new Text("index"), new Text(fastaName + "\t" + tallymerIndexName));
	}
}
