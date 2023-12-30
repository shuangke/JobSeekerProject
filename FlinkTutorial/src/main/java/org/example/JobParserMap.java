package org.example;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.kinesis.shaded.com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordObjectMapper;
import java.util.Map;
public class JobParserMap implements MapFunction<String, Job> {
    @Override
    public Job map(String s) throws Exception {
        RecordObjectMapper mapper = (RecordObjectMapper) new RecordObjectMapper()
                .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Record record = mapper.readValue(s, Record.class);
        String eventId = record.getEventName();
        Map<String, AttributeValue> attributes = null;
        if (eventId.equals("INSERT") || eventId.equals("MODIFY")) {
            attributes = record.getDynamodb().getNewImage();
        } else if (eventId.equals("REMOVE")){
            attributes = record.getDynamodb().getOldImage();
        } else {
            System.out.println("invalid query for JobParserMap");
            return null;
        }

        String jobId = attributes.get("jobId").getS();
        String jobTitle = attributes.get("jobTitle").getS();
        String jobDescription = attributes.get("jobDescription").getS();
        String searchableContent = jobTitle + jobDescription;
        System.out.println("successfully pased a job, jobId: " + jobId + ", jobTitle " +
                jobTitle + " jobDescription " + jobDescription);
        return new Job(jobId, jobTitle, jobDescription, searchableContent);
    }
}
