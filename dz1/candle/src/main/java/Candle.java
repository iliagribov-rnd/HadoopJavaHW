import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Pattern;
import java.lang.Math;
import java.util.HashMap;
import java.util.Map;

class TradeWritable implements Writable {

    long timeMs;
    long idDeal;
    double price;

    TradeWritable() {}

    TradeWritable(long timeMs, long idDeal, double price) {
        this.timeMs = timeMs;
        this.idDeal = idDeal;
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timeMs);
        out.writeLong(this.idDeal);
        out.writeDouble(this.price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.timeMs = in.readLong();
        this.idDeal = in.readLong();
        this.price = in.readDouble();
    }
}

public class Candle {

    public static class CandleMapper
        extends Mapper<Object, Text, Text, TradeWritable> {

        private long widthMs;
        private Date dateFrom, dateTo;
        private long timeFrom, timeTo;
        private Pattern secRe;
        private Map<String, Integer> idxMapper;
        private Map<String, Integer> idxMapperDefault;
        private Map<String, Map<String, Integer>> fileMapper;
        private int candleNumStrLen;
        private static SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");

        static Date strToDate(String dateStr) {
            int year = Integer.parseInt(dateStr.substring(0, 4));
            int month = Integer.parseInt(dateStr.substring(4, 6));
            int day = Integer.parseInt(dateStr.substring(6, 8));
            Calendar cal = Calendar.getInstance();
            cal.set(year, month - 1, day, 0, 0, 0);
            cal.set(Calendar.MILLISECOND, 0);
            return cal.getTime();
        }

        static long strWithoutMsToMs(String time) {
            long hours = Long.parseLong(time.substring(0, 2));
            long minutes = Long.parseLong(time.substring(2, 4));
            long totalMs = hours * 3600000L + minutes * 60000L;
            return totalMs;
        }

        static long strWithMsToMs(String time) {
            long headTotalMs = CandleMapper.strWithoutMsToMs(time.substring(0, 4));
            long seconds = Long.parseLong(time.substring(4, 6));
            long mss = Long.parseLong(time.substring(6, 9));
            long totalMs = headTotalMs + seconds * 1000L + mss;
            return totalMs;
        }

        static Map<String, Integer> parseHeader(String header) {
            String[] parts = header.split(",");
            Map<String, Integer> idxMapper = new HashMap<String, Integer>();
            for (int idx = 0; idx < parts.length; idx++) {
                String colName = parts[idx].trim();
                idxMapper.put(colName, idx);
            }
            return idxMapper;
        }

        @Override
        public void setup(Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            this.widthMs = conf.getLong("candle.width", 300000L);
            this.dateFrom = CandleMapper.strToDate(conf.get("candle.date.from", "19000101"));
            this.dateTo = CandleMapper.strToDate(conf.get("candle.date.to", "20200101"));
            this.timeFrom = CandleMapper.strWithoutMsToMs(conf.get("candle.time.from", "1000"));
            this.timeTo = CandleMapper.strWithoutMsToMs(conf.get("candle.time.to", "1800"));
            this.secRe = Pattern.compile(conf.get("candle.securities", ".*"));
            long candleMax = CandleMapper.strWithoutMsToMs(conf.get("candle.time.to", "1800")) / this.widthMs + 1;
            this.candleNumStrLen = Math.max(1, String.valueOf(candleMax).length());
            this.fileMapper = new HashMap<String, Map<String, Integer>>();
            this.idxMapperDefault = new HashMap<String, Integer>();
            this.idxMapperDefault.put("#SYMBOL", 0);
            this.idxMapperDefault.put("MOMENT", 2);
            this.idxMapperDefault.put("ID_DEAL", 3);
            this.idxMapperDefault.put("PRICE_DEAL", 4);
        }

        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            FileSplit split = (FileSplit) context.getInputSplit();
            String filePath = split.getPath().toString();
            if (!this.fileMapper.containsKey(filePath)) {
                FileSystem fs = split.getPath().getFileSystem(context.getConfiguration());
                FSDataInputStream stream = fs.open(split.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
                String header = reader.readLine();
                reader.close();
                if (header != null && header.contains("#SYMBOL")) {
                    this.fileMapper.put(filePath, CandleMapper.parseHeader(header));
                }
                else {
                    this.fileMapper.put(filePath, this.idxMapperDefault);
                }
            }
            this.idxMapper = fileMapper.get(filePath);

            String trade_line = value.toString().trim();
            if (trade_line.isEmpty() || trade_line.contains("#SYMBOL")) return;

            String[] trade_params = trade_line.split(",");
            if (trade_params.length < 8) return;

            String dateStr = trade_params[this.idxMapper.get("MOMENT")].substring(0, 8);
            Date date = CandleMapper.strToDate(dateStr);
            if (date.compareTo(this.dateFrom) < 0 || date.compareTo(this.dateTo) >= 0) return;

            long totalMs = CandleMapper.strWithMsToMs(trade_params[this.idxMapper.get("MOMENT")].substring(8));
            if (totalMs < this.timeFrom || totalMs >= this.timeTo) return;

            long candleNum = totalMs / this.widthMs;
            String tool = trade_params[this.idxMapper.get("#SYMBOL")];
            if (!this.secRe.matcher(tool).matches()) return;

            String candleNumStr = String.format("%0" + this.candleNumStrLen+ "d", candleNum);

            String candleKeyStr = tool + "|" + formatter.format(date) + "|" + candleNumStr;
            Text candleKey = new Text(candleKeyStr);

            long id_deal = Long.parseLong(trade_params[this.idxMapper.get("ID_DEAL")]);
            double price = Double.parseDouble(trade_params[this.idxMapper.get("PRICE_DEAL")]);

            context.write(candleKey, new TradeWritable(totalMs, id_deal, price));
        }
    }

    public static class TimePartitioner extends Partitioner<Text, TradeWritable> implements Configurable{

        private long reducerSz;
        private long candleBase;

        static long parseHM(String hhmm) {
            long hh = Long.parseLong(hhmm.substring(0, 2));
            long mm = Long.parseLong(hhmm.substring(2, 4));
            long mss = hh * 3600000L + mm * 60000L;
            return mss;
        }

        @Override
        public void setConf(Configuration conf) {
            long widthMs = conf.getLong("candle.width", 300000L);
            long numReducers = conf.getLong("candle.num.reducers", 1L);
            long startMs = TimePartitioner.parseHM(conf.get("candle.time.from", "1000"));
            long endMs = TimePartitioner.parseHM(conf.get("candle.time.to", "1800"));
            long timeDiff = endMs - startMs;
            long candlesCnt = ((timeDiff % widthMs) == 0)  ? (timeDiff / widthMs) : (timeDiff / widthMs + 1);
            this.reducerSz = ((candlesCnt % numReducers) == 0) ? (candlesCnt / numReducers) : (candlesCnt / numReducers + 1);
            this.candleBase = TimePartitioner.parseHM(conf.get("candle.time.from", "1000")) / widthMs;
        }

        @Override
        public Configuration getConf() {
            return null;
        }

        @Override
        public int getPartition(Text key, TradeWritable value, int numReducers) {
            if (numReducers == 1) return 0;
            String[] parts = key.toString().split("\\|");
            long candleNum = Long.parseLong(parts[2]);
            long candleDiff = candleNum - this.candleBase;
            long reducerToSend = candleDiff / this.reducerSz;
            return (int) Math.min(reducerToSend, numReducers - 1);
        }
    }

    public static class CandleAggregate
        extends Reducer<Text, TradeWritable, NullWritable, Text> {

        private long widthMs;
        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.widthMs = conf.getLong("candle.width", 300000L);
            this.mos = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        public void reduce(
            Text key,
            Iterable<TradeWritable> values,
            Context context
        ) throws IOException, InterruptedException {
            double highPrice = Double.NEGATIVE_INFINITY;
            double lowPrice = Double.POSITIVE_INFINITY;

            double openPrice = 0.0;
            double closePrice = 0.0;

            long openTms = Long.MAX_VALUE;
            long openIdd = Long.MAX_VALUE;
            long closeTms = Long.MIN_VALUE;
            long closeIdd = Long.MIN_VALUE;

            for (TradeWritable v : values) {
                final double price = v.price;
                final long tms = v.timeMs;
                final long idd = v.idDeal;

                if (price > highPrice) highPrice = price;
                if (price < lowPrice) lowPrice = price;

                if (tms < openTms || (tms == openTms && idd < openIdd)) {
                    openTms = tms;
                    openIdd = idd;
                    openPrice = price;
                }
                if (tms > closeTms || (tms == closeTms && idd > closeIdd)) {
                    closeTms = tms;
                    closeIdd = idd;
                    closePrice = price;
                }
            }

            String[] parts = key.toString().split("\\|");
            String tool = parts[0];
            String date = parts[1];
            long candleNum = Long.parseLong(parts[2]);

            long candleStartMs = candleNum * this.widthMs;
            long hours = candleStartMs / 3600000;
            long minutes = (candleStartMs % 3600000) / 60000;
            long seconds = (candleStartMs % 60000) / 1000;
            long ms = candleStartMs % 1000;
            String tmsStr = String.format("%02d%02d%02d%03d", hours, minutes, seconds, ms);
            date += tmsStr;

            String line =
                tool + "," +
                date + "," +
                String.format(java.util.Locale.US, "%.1f", openPrice) + "," +
                String.format(java.util.Locale.US, "%.1f", highPrice) + "," +
                String.format(java.util.Locale.US, "%.1f", lowPrice) + "," +
                String.format(java.util.Locale.US, "%.1f", closePrice);

            mos.write(NullWritable.get(), new Text(line), tool);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] actualArgs = args;
        if (args.length > 0 && args[0].equalsIgnoreCase("candle")) {
            actualArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        String[] remaining = new GenericOptionsParser(conf, actualArgs).getRemainingArgs();
        System.out.println("Arguments:");
        for (int i = 0; i < remaining.length; i++) {
            System.out.println("args[" + i + "] = " + remaining[i]);
        }
        if (remaining.length < 2 || remaining.length > 3) {
            System.err.println("input or output paths are lack or too much arguments!");
            System.exit(2);
        }

        conf.setIfUnset("candle.width", "300000");
        conf.setIfUnset("candle.securities", ".*");
        conf.setIfUnset("candle.date.from", "19000101");
        conf.setIfUnset("candle.date.to", "20200101");
        conf.setIfUnset("candle.time.from", "1000");
        conf.setIfUnset("candle.time.to", "1800");
        conf.setIfUnset("candle.num.reducers", "1");
        System.out.println("Reducers num: " + conf.getInt("candle.num.reducers", 1));

        Job job = Job.getInstance(conf, "candle");
        job.setJarByClass(Candle.class);

        job.setMapperClass(CandleMapper.class);
        job.setPartitionerClass(TimePartitioner.class);
        job.setReducerClass(CandleAggregate.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TradeWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));

        int idxIn, idxOut;
        if (remaining.length == 3) {
            idxIn = 1;
            idxOut = 2;
        }
        else {
            idxIn = 0;
            idxOut = 1;
        }
        FileInputFormat.addInputPath(job, new Path(remaining[idxIn]));
        FileOutputFormat.setOutputPath(job, new Path(remaining[idxOut]));

        long t0 = System.nanoTime();
        boolean ok = job.waitForCompletion(true);
        long t1 = System.nanoTime();
        long elapsedMs = (t1 - t0) / 1000000L;
        System.out.println("RUNTIME_MS=" + elapsedMs);
        System.exit(ok ? 0 : 1);
    }
}
