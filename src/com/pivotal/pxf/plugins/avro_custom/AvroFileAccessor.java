package com.pivotal.pxf.plugins.avro_custom;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.plugins.hdfs.HdfsSplittableDataAccessor;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;

public class AvroFileAccessor extends HdfsSplittableDataAccessor {

	private AvroWrapper<GenericRecord> avroWrapper = null;
	@SuppressWarnings({ "rawtypes", "unchecked" })
	
	public AvroFileAccessor(InputData input) throws Exception{
		super(input, new AvroInputFormat());
		Schema schema = HdfsUtilities.getAvroSchema(this.conf, this.inputData.getDataSource());
		AvroJob.setInputSchema(this.jobConf, schema);
		this.avroWrapper = new AvroWrapper();
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	protected Object getReader(JobConf jobConf, InputSplit split)
			throws IOException {
		return new AvroRecordReader(jobConf,(FileSplit)split);
	}
	
	public OneRow readNextObject() throws IOException{
		this.avroWrapper.datum(null);
		if(this.reader.next(this.avroWrapper, NullWritable.get()))
			return new OneRow(null, this.avroWrapper.datum());
		if(this.getNextSplit())
			return this.reader.next(this.avroWrapper, NullWritable.get()) ? new OneRow(null, this.avroWrapper.datum()) : null;
		return null;
		
	}

}
