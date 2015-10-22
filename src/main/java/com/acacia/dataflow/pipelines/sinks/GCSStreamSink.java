package com.acacia.dataflow.pipelines.sinks;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriteOperation;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriter;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.common.base.Charsets;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class GCSStreamSink  {



    public static class Bound<T> extends FileBasedSink<T>{


        public Bound(String baseOutputFilename, String extension) {
            super(baseOutputFilename, extension);
        }

        public Bound(String baseOutputFilename, String extension, String fileNamingTemplate) {
            super(baseOutputFilename, extension, fileNamingTemplate);
        }

        @Override
        public void validate(PipelineOptions options) {
            super.validate(options);
        }

        @Override
        public FileBasedWriteOperation<T> createWriteOperation(PipelineOptions pipelineOptions) {
            return new GCSStreamWriteOperation<>(this);
        }


    }



    public static class GCSStreamWriteOperation<T> extends FileBasedWriteOperation<T>{
        public GCSStreamWriteOperation(FileBasedSink<T> sink) {
            super(sink);
        }

        public GCSStreamWriteOperation(FileBasedSink<T> sink, String baseTemporaryFilename) {
            super(sink, baseTemporaryFilename);
        }

        public GCSStreamWriteOperation(FileBasedSink<T> sink, String baseTemporaryFilename, TemporaryFileRetention temporaryFileRetention) {
            super(sink, baseTemporaryFilename, temporaryFileRetention);
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {
            super.initialize(options);
        }

        @Override
        public void finalize(Iterable<FileBasedSink.FileResult> writerResults, PipelineOptions options) throws Exception {
            super.finalize(writerResults, options);
        }

        @Override
        public Coder<FileBasedSink.FileResult> getWriterResultCoder() {
            return super.getWriterResultCoder();
        }

        @Override
        public GCSStreamSink.Bound<T> getSink() {
            return (GCSStreamSink.Bound<T>) super.getSink();
        }

        @Override
        public FileBasedSink.FileBasedWriter<T> createWriter(PipelineOptions pipelineOptions) throws Exception {
            return new GCSStreamWriter<>(this);
        }
    }



    protected static final class GCSStreamWriter<T> extends FileBasedWriter<T>{

        WritableByteChannel bc = null;


        public GCSStreamWriter(FileBasedSink.FileBasedWriteOperation<T> writeOperation) {
            super(writeOperation);
        }

        @Override
        protected void writeHeader() throws Exception {
            super.writeHeader();
        }

        @Override
        protected void writeFooter() throws Exception {
            super.writeFooter();
        }

        @Override
        public GCSStreamWriteOperation<T> getWriteOperation() {
            return (GCSStreamWriteOperation<T>) super.getWriteOperation();
        }

        @Override
        protected void prepareWrite(WritableByteChannel writableByteChannel) throws Exception {
            bc = writableByteChannel;
            //out = new BufferedWriter(writableByteChannel);

        }

        @Override
        public void write(T t) throws Exception {

            String s = (String) t;
            byte[] bytes = s.getBytes(Charsets.UTF_8);

            System.out.println("t: " + s);
            bc.write(ByteBuffer.wrap(bytes));
        }
    }



}
