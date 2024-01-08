package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;

import java.util.Properties;


public class KinesisEventMonitor {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, "us-west-2");
        properties.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, CredentialProperties.accessKey);
        properties.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, CredentialProperties.secretKey);
        DataStream<String> stream = env.addSource(new FlinkKinesisConsumer<>(
                "jobRunner", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<Job> kinesisStreamReconstructToJobObjectStream = stream.map(new JobParserMap());
        SingleOutputStreamOperator<Job> jobStreamAfterNomalization = kinesisStreamReconstructToJobObjectStream.map(new JobSearchableContentNormalizationMap());
        jobStreamAfterNomalization.print();
        jobStreamAfterNomalization.addSink(new LuceneIndexSink()).setParallelism(1);
        env.execute("Kinesis Flink Job");
    }
}
