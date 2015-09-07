package com.pivotal.pxf.plugins.avro_custom;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;

import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.hdfs.utilities.DataSchemaException;
import com.pivotal.pxf.plugins.hdfs.utilities.HdfsUtilities;
import com.pivotal.pxf.plugins.hdfs.utilities.RecordkeyAdapter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class AvroResolver extends Plugin implements ReadResolver{
	private GenericRecord avroRecord = null;
	private DatumReader <GenericRecord> reader = null;
	private BinaryDecoder decoder = null;	
	private List<Schema.Field> fields = null;
	private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();
	private static final String MAPKEY_DELIM = ":";
	private static final String RECORDKEY_DELIM = ":";
	private static final String COLLECTION_DELIM = ",";
	private String collectionDelim;
	private String mapkeyDelim;
	private String recordkeyDelim;
	
	public AvroResolver(InputData input) throws IOException{
		super(input);
		
		Schema schema = isAvroFile() ? HdfsUtilities.getAvroSchema(new Configuration(), input.getDataSource()) : new Schema.Parser().parse(openExternalSchema());
		this.reader = new GenericDatumReader(schema);
		this.fields = schema.getFields();
		this.collectionDelim = (input.getUserProperty("COLLECTION_DELIM") == null ? "," : input.getUserProperty("COLLECTION_DELIM"));
		this.mapkeyDelim = (input.getUserProperty("MAPKEY_DELIM") == null ? ":" : input.getUserProperty("MAPKEY_DELIM"));
		this.recordkeyDelim = (input.getUserProperty("RECORDKEY_DELIM") == null ? ":" : input.getUserProperty("RECORDKEY_DELIM"));
	}
	
	public List<OneField> getFields(OneRow row) throws Exception{
		this.avroRecord = makeAvroRecord(row.getData(), this.avroRecord);
		List<OneField> record = new LinkedList<OneField>();
		int recordkeyIndex = this.inputData.getRecordkeyColumn() == null ? -1 : this.inputData.getRecordkeyColumn().columnIndex();
		int currentIndex = 0;

	    for (Schema.Field field : this.fields){
	    	if (currentIndex == recordkeyIndex)
	    		currentIndex += this.recordkeyAdapter.appendRecordkeyField(record, this.inputData, row);
			currentIndex += populateRecord(record, this.avroRecord.get(field.name()), field.schema());
	    }
	    return record;
	}	

	@SuppressWarnings("unchecked")
	private int populateRecord(List<OneField> record, Object fieldValue, Schema fieldSchema) throws IllegalAccessException{
		Schema.Type fieldType = fieldSchema.getType();
		int ret = 0;
		Object value;
		/*RECORD*, ENUM*, ARRAY*, MAP*, UNION*, FIXED*, STRING*, BYTES*,INT*, LONG*, FLOAT*, DOUBLE*, BOOLEAN*, NULL*;*/
		switch ( fieldType) {
		case FIXED:
			List fixedRecord = new LinkedList();
			if(fieldValue!=null){
				ret = setFixedField(fixedRecord, fieldValue, fieldSchema);
				addOneFieldToRecord(record,DataType.TEXT, String.format("{%s}", new Object[] { HdfsUtilities.toString(fixedRecord, this.collectionDelim) }));
			}
			else{
				ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
			}
			break;
		case ARRAY:
			@SuppressWarnings("rawtypes")
			List listRecord = new LinkedList();
			if (fieldValue!=null){
				ret = setArrayField(listRecord, fieldValue, fieldSchema);
				addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]", new Object[] { HdfsUtilities.toString(listRecord, this.collectionDelim) }));
			}
			else{
				ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
			}
			break;
		case MAP:
		    @SuppressWarnings("rawtypes")
			List mapRecord = new LinkedList();
		    if (fieldValue!=null){
		    	ret = setMapField(mapRecord, fieldValue, fieldSchema);
		    	addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}", new Object[] { HdfsUtilities.toString(mapRecord, this.collectionDelim) }));
		    }
		    else{
		    	ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
		    }

		    break;
		case RECORD:
		    @SuppressWarnings("rawtypes")
			List recRecord = new LinkedList();
		    if(fieldValue!=null){
		    	ret = setRecordField(recRecord, fieldValue, fieldSchema);
		    	addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}", new Object[] { HdfsUtilities.toString(recRecord, this.collectionDelim) }));
		    }
		    else{
		    	ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
		    }
		    break;
		case UNION:
			/* need to handle null here */
		    int unionIndex = GenericData.get().resolveUnion(fieldSchema, fieldValue);
		    if (fieldValue == null)
		    	unionIndex ^= 1;
		    ret = populateRecord(record, fieldValue, (Schema)fieldSchema.getTypes().get(unionIndex));
		    break;
		case STRING:
		    value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.TEXT, value);
		    break;
		case INT:
		    value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.INTEGER, value);
		    break;
		case FLOAT:
		    value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.REAL, value);
		    break;
	    case BYTES:
		    value = fieldValue != null ? String.format("%s", new Object[] { fieldValue }) : null;
		    ret = addOneFieldToRecord(record, DataType.TEXT, value);
		    break;
		case LONG:
		    value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.BIGINT, value);
		    break;
	    case DOUBLE:
	    	value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.FLOAT8, value);
		    break;
	    case ENUM:
	    	value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.BYTEA, value);
		    break;
	    case BOOLEAN:
	    	value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.BOOLEAN, value);
		    break;
	    case NULL:
	    	value = fieldValue != null ? fieldValue : null;
		    ret = addOneFieldToRecord(record, DataType.BYTEA, value);
		    break;
		}
		return ret;
}

	private int setFixedField(List fixedRecord, Object fieldValue,
			Schema fieldSchema) {
		// TODO Auto-generated method stub
		return 0;
	}

	private int addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType, Object val) {
	    OneField oneField = new OneField();
	    oneField.type = gpdbWritableType.getOID(); //this returns text in this case
	    switch (gpdbWritableType) {
	    case BYTEA:
	      if ((val instanceof ByteBuffer)) {
	        oneField.val = ((ByteBuffer)val).array();
	      }
	      else {
	        oneField.val = val != null ? ((GenericData.Fixed)val).bytes(): null;//this is why we lose 2 null fields
	      }
	      break;
	    default:
	      oneField.val = val;
	    }

	    record.add(oneField);
	    return 1;
	}

	private int setRecordField(List<OneField> record, Object value, Schema recSchema) throws IllegalAccessException{
		GenericRecord rec = (GenericData.Record)value;
	    Schema fieldKeySchema = Schema.create(Schema.Type.STRING);
	    int currentIndex = 0;
	    for (Schema.Field field : recSchema.getFields()) {
	      Schema fieldSchema = field.schema();
	      Schema.Type fieldType = fieldSchema.getType();
	      Object fieldValue = rec.get(field.name());
	      List complexRecord = new LinkedList();
	      populateRecord(complexRecord, field.name(), fieldKeySchema);
	      populateRecord(complexRecord, fieldValue, fieldSchema);
	      addOneFieldToRecord(record, DataType.TEXT, HdfsUtilities.toString(complexRecord, this.recordkeyDelim));
	      currentIndex++;
	    }
	    return currentIndex;
	}

	private int setMapField(List<OneField> record, Object value,Schema mapSchema) throws IllegalAccessException{
	    Schema keySchema = Schema.create(Schema.Type.STRING);
	    Schema valueSchema = mapSchema.getValueType();
	    @SuppressWarnings("rawtypes")
		Map avroMap = (Map)value;
	    Set<Entry> entries = avroMap.entrySet();
	    for (Entry entry : entries) {
	      List complexRecord = new LinkedList();
	      populateRecord(complexRecord, entry.getKey(), keySchema);
	      populateRecord(complexRecord, entry.getValue(), valueSchema);
	      addOneFieldToRecord(record, DataType.TEXT, HdfsUtilities.toString(complexRecord, this.mapkeyDelim));
	      System.out.println("handled");//never get this!
	    }
	    
	    return avroMap.size();
	}

	private int setArrayField(List<OneField> record, Object value,Schema arraySchema) throws IllegalAccessException{
		Schema typeSchema = arraySchema.getElementType();
		GenericData.Array array = (GenericData.Array)value;
	    int length = array.size();
	    for (int i = 0; i < length; i++) {
	    	populateRecord(record, array.get(i), typeSchema);
		}
	    return length;
	}

	private GenericRecord makeAvroRecord(Object data, GenericRecord reuseRecord) throws IOException{
		if (this.isAvroFile())
			return (GenericRecord)data;
		byte[] bytes = ((BytesWritable)data).getBytes();
		this.decoder = DecoderFactory.get().binaryDecoder(bytes, this.decoder);
		return(GenericRecord)this.reader.read(reuseRecord, this.decoder);
	}

	private boolean isAvroFile(){
		return this.inputData.getAccessor().toLowerCase().contains("avro");
	}
	
	private InputStream openExternalSchema() throws IOException{
		String schemaName = this.inputData.getUserProperty("DATA-SCHEMA");
		if(schemaName == null)
			throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_INDICATED, new String[] {getClass().getName()});
		if (getClass().getClassLoader().getResource(schemaName) == null)
		    throw new DataSchemaException(DataSchemaException.MessageFmt.SCHEMA_NOT_ON_CLASSPATH, new String[] { schemaName });
		ClassLoader loader = getClass().getClassLoader();
		return loader.getResourceAsStream(schemaName);
	}

}
