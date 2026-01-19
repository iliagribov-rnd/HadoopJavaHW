import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

class BlockWritable implements Writable {

    public char tag;
    public int idx;
    public int jdx;
    public double val;

    BlockWritable() {}
    BlockWritable(char tag, int idx, int jdx, double val) {
        this.tag = tag;
        this.idx = idx;
        this.jdx = jdx;
        this.val = val;
    }
    BlockWritable(int idx, int jdx, double val) {
        this.tag = '\0';
        this.idx = idx;
        this.jdx = jdx;
        this.val = val;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeChar(this.tag);
        out.writeInt(this.idx);
        out.writeInt(this.jdx);
        out.writeDouble(this.val);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.tag = in.readChar();
        this.idx = in.readInt();
        this.jdx = in.readInt();
        this.val = in.readDouble();
    }
}

class retSizes {
    int[] fstSize;
    int[] sndSize;
    int[] fstSizeLine;
    int[] sndSizeLine;
    int[] fstSizeLineLst;
    int[] sndSizeLineLst;
    int numLines;
    char[] tags;

    public retSizes(){}
    public retSizes(boolean init){
        if (init) {
            this.fstSize = new int[2];
            this.sndSize = new int[2];
            this.fstSizeLine = new int[2];
            this.sndSizeLine = new int[2];
            this.fstSizeLineLst = new int[2];
            this.sndSizeLineLst = new int[2];
            this.tags = new char[3];
        }
    }
}

public class mm {

    public static int ceilBlock(int sideSize, int sizeBlock) {
        int integralPart = sideSize / sizeBlock;
        return (sideSize % sizeBlock == 0) ? integralPart : (integralPart + 1);
    }

    public static retSizes computeSizes(Configuration conf) {
        retSizes retSz = new retSizes(true);
        retSz.fstSize[0] = Integer.parseInt(conf.get("mm.fst.size.h", "800"));
        retSz.fstSize[1] = Integer.parseInt(conf.get("mm.fst.size.w", "800"));
        retSz.sndSize[0] = Integer.parseInt(conf.get("mm.snd.size.h", "800"));
        retSz.sndSize[1] = Integer.parseInt(conf.get("mm.snd.size.w", "800"));
        retSz.numLines = Integer.parseInt(conf.get("mm.groups", "1"));
        retSz.tags = conf.get("mm.tags", "ABC").toCharArray();

        retSz.fstSizeLine[0] = ceilBlock(retSz.fstSize[0], retSz.numLines);
        retSz.fstSizeLine[1] = ceilBlock(retSz.fstSize[1], retSz.numLines);
        retSz.sndSizeLine[0] = ceilBlock(retSz.sndSize[0], retSz.numLines);
        retSz.sndSizeLine[1] = ceilBlock(retSz.sndSize[1], retSz.numLines);
        retSz.fstSizeLineLst[0] = retSz.fstSize[0] - (retSz.numLines - 1) * retSz.fstSizeLine[0];
        retSz.fstSizeLineLst[1] = retSz.fstSize[1] - (retSz.numLines - 1) * retSz.fstSizeLine[1];
        retSz.sndSizeLineLst[0] = retSz.sndSize[0] - (retSz.numLines - 1) * retSz.sndSizeLine[0];
        retSz.sndSizeLineLst[1] = retSz.sndSize[1] - (retSz.numLines - 1) * retSz.sndSizeLine[1];

        return retSz;
    }

    public static class MatrixBlockMapper
        extends Mapper<Object, Text, Text, BlockWritable> {

        private int numLines;
        private int[] fstSizeLine = new int[2];
        private int[] sndSizeLine = new int[2];
        private char[] tags;

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            retSizes retSz = computeSizes(conf);
            this.fstSizeLine = retSz.fstSizeLine;
            this.sndSizeLine = retSz.sndSizeLine;
            this.tags = retSz.tags;
            this.numLines = retSz.numLines;
        }

        @Override
        public void map(
            Object key,
            Text value,
            Context context
        ) throws IOException, InterruptedException{
            String[] matrix_elem = value.toString().split("\\t");
            char tag = matrix_elem[0].charAt(0);
            int idx = Integer.parseInt(matrix_elem[1]);
            int jdx = Integer.parseInt(matrix_elem[2]);
            double val = Double.parseDouble(matrix_elem[3]);

            if (tag == tags[0]) {
                int idxBlockNum, jdxBlockNum;
                int idxLocal, jdxLocal;
                idxBlockNum = idx / this.fstSizeLine[0];
                jdxBlockNum = jdx / this.fstSizeLine[1];
                idxLocal = idx % this.fstSizeLine[0];
                jdxLocal = jdx % this.fstSizeLine[1];
                for (int kdxBlockNum = 0; kdxBlockNum < this.numLines; kdxBlockNum++) {
                    Text blockKey = new Text(
                        Integer.toString(idxBlockNum) + "|" +
                        Integer.toString(jdxBlockNum) + "|" +
                        Integer.toString(kdxBlockNum)
                    );
                    context.write(blockKey, new BlockWritable(tag, idxLocal, jdxLocal, val));
                }
            }
            else if (tag == tags[1]) {
                int jdxBlockNum, kdxBlockNum;
                int jdxLocal, kdxLocal;
                jdxBlockNum = idx / this.sndSizeLine[0];
                kdxBlockNum = jdx / this.sndSizeLine[1];
                jdxLocal = idx % this.sndSizeLine[0];
                kdxLocal = jdx % this.sndSizeLine[1];
                for (int idxBlockNum = 0; idxBlockNum < this.numLines; idxBlockNum++) {
                    Text blockKey = new Text(
                        Integer.toString(idxBlockNum) + "|" +
                        Integer.toString(jdxBlockNum) + "|" +
                        Integer.toString(kdxBlockNum)
                    );
                    context.write(blockKey, new BlockWritable(tag, jdxLocal, kdxLocal, val));
                }
            }
        }
    }

    public static class MatrixBlockAggregate
        extends Reducer<Text, BlockWritable, Text, BlockWritable> {

        private int numLines;
        private int[] fstSizeLine = new int[2];
        private int[] sndSizeLine = new int[2];
        private int[] fstSizeLineLst = new int[2];
        private int[] sndSizeLineLst = new int[2];
        private char[] tags;

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            retSizes retSz = computeSizes(conf);
            this.fstSizeLine = retSz.fstSizeLine;
            this.sndSizeLine = retSz.sndSizeLine;
            this.fstSizeLineLst = retSz.fstSizeLineLst;
            this.sndSizeLineLst = retSz.sndSizeLineLst;
            this.tags = retSz.tags;
            this.numLines = retSz.numLines;
        }

        @Override
        public void reduce(
            Text key,
            Iterable<BlockWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\\|");
            int idxBlockNum = Integer.parseInt(parts[0]);
            int jdxBlockNum = Integer.parseInt(parts[1]);
            int kdxBlockNum = Integer.parseInt(parts[2]);
            int[] fstSizeBlock = new int[2];
            fstSizeBlock[0] = (idxBlockNum == this.numLines - 1) ? this.fstSizeLineLst[0] : this.fstSizeLine[0];
            fstSizeBlock[1] = (jdxBlockNum == this.numLines - 1) ? this.fstSizeLineLst[1] : this.fstSizeLine[1];
            int[] sndSizeBlock = new int[2];
            sndSizeBlock[0] = (jdxBlockNum == this.numLines - 1) ? this.sndSizeLineLst[0] : this.sndSizeLine[0];
            sndSizeBlock[1] = (kdxBlockNum == this.numLines - 1) ? this.sndSizeLineLst[1] : this.sndSizeLine[1];
            double[][] fstMatrix = new double[fstSizeBlock[0]][fstSizeBlock[1]];
            double[][] sndMatrix = new double[sndSizeBlock[0]][sndSizeBlock[1]];

            for (BlockWritable v : values) {
                char tag  = v.tag;
                if (tag == this.tags[0]) {
                    fstMatrix[v.idx][v.jdx] = v.val;
                }
                else {
                    sndMatrix[v.idx][v.jdx] = v.val;
                }
            }

            Text outKey = new Text(
                Integer.toString(idxBlockNum) + "|" +
                Integer.toString(kdxBlockNum)
            );
            int commonDim = Math.min(fstSizeBlock[1], sndSizeBlock[0]);
            for (int iM = 0; iM < fstSizeBlock[0]; iM++) {
                for (int kM = 0; kM < sndSizeBlock[1]; kM++) {
                    double sm = 0;
                    for (int jM = 0; jM < commonDim; jM++) {
                        sm += (fstMatrix[iM][jM] * sndMatrix[jM][kM]);
                    }
                    if (sm != 0.0) {
                        context.write(outKey, new BlockWritable(iM, kM, sm));
                    }
                }
            }
        }
    }

    public static class MatrixSumMapper
        extends Mapper<Text, BlockWritable, Text, BlockWritable> {
        @Override
        protected void map(
            Text key,
            BlockWritable value,
            Context context
        ) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class MatrixSumAggregate
        extends Reducer<Text, BlockWritable, NullWritable, Text> {

        private int numLines;
        private int[] fstSizeLine = new int[2];
        private int[] sndSizeLine = new int[2];
        private int[] fstSizeLineLst = new int[2];
        private int[] sndSizeLineLst = new int[2];
        private char[] tags;
        private String floatFormat;

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            retSizes retSz = computeSizes(conf);
            this.fstSizeLine = retSz.fstSizeLine;
            this.sndSizeLine = retSz.sndSizeLine;
            this.fstSizeLineLst = retSz.fstSizeLineLst;
            this.sndSizeLineLst = retSz.sndSizeLineLst;
            this.tags = retSz.tags;
            this.numLines = retSz.numLines;

            this.floatFormat = conf.get("mm.float-format", "%.3f");
        }

        @Override
        protected void reduce(
            Text key,
            Iterable<BlockWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            String[] parts = key.toString().split("\\|");
            int idxBlockNum = Integer.parseInt(parts[0]);
            int kdxBlockNum = Integer.parseInt(parts[1]);
            int[] sizeBlock = new int[2];
            sizeBlock[0] = (idxBlockNum == this.numLines - 1) ? this.fstSizeLineLst[0] : this.fstSizeLine[0];
            sizeBlock[1] = (kdxBlockNum == this.numLines - 1) ? this.sndSizeLineLst[1] : this.sndSizeLine[1];
            double[][] resMatrix = new double[sizeBlock[0]][sizeBlock[1]];
            for (BlockWritable v : values) {
                resMatrix[v.idx][v.jdx] += v.val;
            }

            int rowBase = idxBlockNum * this.fstSizeLine[0];
            int colBase = kdxBlockNum * this.sndSizeLine[1];
            char lstTag = this.tags[2];

            for (int idx = 0; idx < sizeBlock[0]; idx++) {
                int globalIdx = rowBase + idx;
                for (int jdx = 0; jdx < sizeBlock[1]; jdx++) {
                    int globalJdx = colBase + jdx;
                    double val = resMatrix[idx][jdx];
                    if (val != 0.0) {
                        String line = String.format(
                            "%c\t%d\t%d\t" + this.floatFormat,
                            lstTag, globalIdx, globalJdx, val
                        );
                        context.write(NullWritable.get(), new Text(line));
                    }
                }
            }
        }
    }

    static int[] readSizeFile(Configuration conf, Path sizePath) throws IOException {
        FileSystem fs = sizePath.getFileSystem(conf);
        FSDataInputStream stream = fs.open(sizePath);
        InputStreamReader streamReader = new InputStreamReader(stream);
        BufferedReader reader = new BufferedReader(streamReader);
        String line = reader.readLine();
        reader.close();
        String[] sizesStr = line.trim().split("\\t");
        int[] sizesInt = {Integer.parseInt(sizesStr[0]), Integer.parseInt(sizesStr[1])};
        return sizesInt;
    }

    private static void printJobStats(Job... jobs) throws IOException {
        long mapInputRecords = 0;
        long mapOutputRecords = 0;
        long mapInputBytes = 0;
        long mapOutputBytes = 0;
        long reduceInputGroups = 0;
        long reduceShuffleBytes = 0;
        long reduceInputRecords = 0;
        long reduceOutputRecords = 0;
        List<String> jobNames = new ArrayList<>();

        for (Job job : jobs) {
            Counters counters = job.getCounters();
            jobNames.add(job.getJobName());

            mapInputRecords += counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
            mapOutputRecords += counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            mapInputBytes += counters.findCounter("org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter", "BYTES_READ").getValue();
            mapOutputBytes += counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_OUTPUT_BYTES").getValue();
            reduceInputGroups += counters.findCounter(TaskCounter.REDUCE_INPUT_GROUPS).getValue();
            reduceShuffleBytes += counters.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue();
            reduceInputRecords += counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            reduceOutputRecords += counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
        }

        StringBuilder jobNamesStr = new StringBuilder();
        for (int idx = 0; idx < jobNames.size(); idx++) {
            if (idx > 0) {jobNamesStr.append(", ");}
            jobNamesStr.append(jobNames.get(idx));
        }
        System.out.println("Stats for jobs: " + jobNamesStr.toString());
        System.out.println("MAP_INPUT_RECORDS: " + mapInputRecords);
        System.out.println("MAP_OUTPUT_RECORDS: " + mapOutputRecords);
        System.out.println("MAP_INPUT_BYTES: " + mapInputBytes);
        System.out.println("MAP_OUTPUT_BYTES: " + mapOutputBytes);
        System.out.println("REDUCE_INPUT_GROUPS: " + reduceInputGroups);
        System.out.println("REDUCE_SHUFFLE_BYTES: " + reduceShuffleBytes);
        System.out.println("REDUCE_INPUT_RECORDS: " + reduceInputRecords);
        System.out.println("REDUCE_OUTPUT_RECORDS: " + reduceOutputRecords);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remaining = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.out.println("Arguments:");
        for (int i = 0; i < remaining.length; i++) {
            System.out.println("args[" + i + "] = " + remaining[i]);
        }
        if (remaining.length != 3) {
            System.err.println(
                "Problem with given arguments!" + "\n" +
                "Must be <APath> <BPath> <CPath>"
            );
            System.exit(2);
        }

        conf.setIfUnset("mm.groups", "1");
        conf.setIfUnset("mm.tags", "ABC");
        conf.setIfUnset("mm.float-format", "%.3f");
        conf.setIfUnset("mapred.reduce.tasks", "1");

        // args[0] = mm
        Path fstRootPath = new Path(remaining[0]);
        Path fstDataPath = new Path(fstRootPath, "data");
        Path fstSizePath = new Path(fstRootPath, "size");

        Path sndRootPath = new Path(remaining[1]);
        Path sndDataPath = new Path(sndRootPath, "data");
        Path sndSizePath = new Path(sndRootPath, "size");

        Path outRootPath = new Path(remaining[2]);
        Path outDataPath = new Path(outRootPath, "data");
        Path outSizePath = new Path(outRootPath, "size");

        Path tmpPath = new Path(outRootPath, "tmp");

        int[] fstSize = readSizeFile(conf, fstSizePath);
        int[] sndSize = readSizeFile(conf, sndSizePath);
        int minFst = Math.min(fstSize[0], fstSize[1]);
        int minSnd = Math.min(sndSize[0], sndSize[1]);
        if (fstSize[1] != sndSize[0]) {
            System.err.println(
                "Problem with given sizes!" + "\n" +
                "Must be matrixes with following sizes MxN and NxK"
            );
            System.exit(2);
        }
        else if (Math.min(minFst, minSnd) <= 0) {
            System.err.println(
                "Problem with given sizes!" + "\n" +
                "Must be matrixes with following positive sizes MxN and NxK"
            );
            System.exit(2);
        }
        conf.setInt("mm.fst.size.h", fstSize[0]);
        conf.setInt("mm.fst.size.w", fstSize[1]);
        conf.setInt("mm.snd.size.h", sndSize[0]);
        conf.setInt("mm.snd.size.w", sndSize[1]);

        FileSystem fs = outRootPath.getFileSystem(conf);
        if (fs.exists(tmpPath)) {
            fs.delete(tmpPath, true);
        }
        if (fs.exists(outDataPath)) {
            fs.delete(outDataPath, true);
        }

        long startTime = System.currentTimeMillis();
        Job job1 = Job.getInstance(conf, "mm-fst-pass");
        job1.setJarByClass(mm.class);
        job1.setMapperClass(MatrixBlockMapper.class);
        job1.setReducerClass(MatrixBlockAggregate.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(BlockWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(BlockWritable.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job1, fstDataPath);
        FileInputFormat.addInputPath(job1, sndDataPath);
        FileOutputFormat.setOutputPath(job1, tmpPath);
        job1.setNumReduceTasks(conf.getInt("mapred.reduce.tasks", 1));

        boolean ok1 = job1.waitForCompletion(true);
        if (!ok1) {
            System.err.println("First pass has failed");
            System.exit(2);
        }

        Job job2 = Job.getInstance(conf, "mm-snd-pass");
        job2.setJarByClass(mm.class);
        job2.setMapperClass(MatrixSumMapper.class);
        job2.setReducerClass(MatrixSumAggregate.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(BlockWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2, outDataPath);
        job2.setNumReduceTasks(conf.getInt("mapred.reduce.tasks", 1));

        boolean ok2 = job2.waitForCompletion(true);
        if (!ok2) {
            System.err.println("Second pass has failed");
            System.exit(2);
        }
        long endTime = System.currentTimeMillis();

        System.out.println("mm.groups:" + conf.get("mm.groups"));
        System.out.println("mapred.reduce.tasks:" + conf.get("mapred.reduce.tasks"));
        System.out.println("total Time (ms): " + (endTime - startTime));
        printJobStats(job1, job2);

        int[] lstSize = new int[2];
        lstSize[0] = fstSize[0];
        lstSize[1] = sndSize[1];

        FSDataOutputStream out = fs.create(outSizePath, true);
        OutputStreamWriter outStreamer = new OutputStreamWriter(out, "UTF-8");
        BufferedWriter outWriter = new BufferedWriter(outStreamer);
        outWriter.write(Integer.toString(lstSize[0]));
        outWriter.write("\t");
        outWriter.write(Integer.toString(lstSize[1]));
        outWriter.newLine();
        outWriter.close();

        fs.delete(tmpPath, true);
    }
}
