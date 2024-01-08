package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class LuceneIndexSink extends RichSinkFunction<Job> {
    private IndexWriter indexWriter;
    private Directory indexDir;
    private LocalDateTime prevReleaseTime;

    private void initializeIndexWriterIfNotExistOrOpen() {
        if (indexWriter == null || !indexWriter.isOpen()) {
            try {
                indexDir = FSDirectory.open(new File("index-directory").toPath());
                IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
                indexWriter = new IndexWriter(indexDir, new IndexWriterConfig());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public void open(Configuration parameters) {
        try {
            indexDir = FSDirectory.open(new File("index-directory").toPath());
            //Directory directory = FSDirectory.open(Paths.get(indexPath));
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            indexWriter = new IndexWriter(indexDir, new IndexWriterConfig());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteDocumentsByJobId(String jobId) {
        if (indexWriter != null && indexWriter.isOpen()) {
            // Create a term that identifies the field and value to match
            Term term = new Term("jobId", jobId);
            try {
                indexWriter.deleteDocuments(term);
                indexWriter.commit();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public void invoke(Job job, Context context) throws Exception {
        initializeIndexWriterIfNotExistOrOpen();
        if (job.getEventId().equals("REMOVE")) {
            deleteDocumentsByJobId(job.getJobId());
        } else {
            //handles update and add operations
            if (job.getEventId().equals("MODIFY")) {
                deleteDocumentsByJobId(job.getJobId());
            }
            Document doc = new Document();
            doc.add(new StringField("jobId", job.getJobId(), Field.Store.YES));
            doc.add(new StringField("jobTitle", job.getJobTitle(), Field.Store.YES));
            doc.add(new StringField("jobDescription", job.getJobDescription(), Field.Store.YES));
            doc.add(new TextField("searchableContent", job.getSearchableContent(), Field.Store.YES));
            indexWriter.addDocument(doc);
        }
        //if release index
        LocalDateTime curTime = LocalDateTime.now();
        if (prevReleaseTime == null) {
            prevReleaseTime = curTime;
        }
        long minutesDiff = ChronoUnit.MINUTES.between(prevReleaseTime, curTime);
        if (minutesDiff > 2) {
            indexWriter.forceMerge(1);
            System.out.println("Another 2 minutes");
            //uploead s3
            //reinitalize index writer with old path
            initializeIndexWriterIfNotExistOrOpen();
            prevReleaseTime = curTime;
        }
    }
    @Override
    public void close() throws Exception {
        if (indexWriter != null) {
            indexWriter.close();
        }
    }
}
