import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Simple tool to try various file reading strategies and compute average speed
 * 
 */
public class SpeedTest {
	public static final char SEPARATOR_CHAR = ' ';

	private static void usage() {
		System.err.println("usage: SpeedTest path num_tests num_threads bufSize [more bufSize]");
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		// Input:
		//		path: file to load
		//		num_tests: num trials per strategy
		//		num_threads: num threads for the multi-threaded strategy
		//		bufSize: buffer size
		if (args.length < 4)
			usage();

		final String path = args[0];
		final int N = Integer.parseInt(args[1]);
		final int threads = Integer.parseInt(args[2]);
		final int[] bufSizes = new int[args.length - 3];
		for (int i = 3; i < args.length; i++) {
			bufSizes[i-3] = Integer.parseInt(args[i]);
		}

		final int numCores = Runtime.getRuntime().availableProcessors();

		System.out.println("v 1.6");
		System.out.println("Number of Available cores: " + numCores);
		System.out.println("Counting lines...");
		final int numLines = countNumLines(path);
		System.out.printf("File has a size of %dB and contains %d lines \n", new File(path).length(), numLines);

		final AbstractStrategy[] strategies = {
				new BufferCharLoader(N, 1),
				new BufferCharLoader(N, threads),
		};

		for (AbstractStrategy strategy : strategies) {
			Runtime.getRuntime().gc();

			System.out.println("Loading File using "+strategy);

			double min_mean = Double.MAX_VALUE;
			String min_description = "";
			int min_bufSize = 0;

			if (strategy.useBuffer) {
				for (int bufSize : bufSizes) {
					final double mean = strategy.load(path, bufSize);
					if (mean < min_mean) {
						min_mean = mean;
						min_bufSize = bufSize;
						min_description = strategy.describeBest();
					}
				}
			} else {
				min_mean = strategy.load(path, -1);
				min_bufSize = -1;
				min_description = strategy.describeBest();
			}

			displayAvgSpeed("Best Speed for bufSize = "+min_bufSize + min_description, min_mean, numLines);
		}
	}

	/**
	 * parse the file to compute numDP (number of lines)
	 */
	private static int countNumLines(final String path) throws Exception {
		final InputStream is = new FileInputStream(path);
		final BufferedReader in = new BufferedReader(new InputStreamReader(is));

		int numLines = 0;
		while (in.readLine() != null) {
			numLines++;
		}

		in.close();

		return numLines;
	}

	private static void displayAvgSpeed(final String msg, final double time_delta, final int points) {
		System.out.println(String.format(msg + " : %d data points in %.3fs (%,.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	static abstract class AbstractStrategy {

		final int N;
		final boolean useBuffer;

		AbstractStrategy(final int N, final boolean useBuffer) {
			this.N = N;
			this.useBuffer = useBuffer;
		}

		abstract double load(final String path, final int bufSize) throws Exception;

		public String describeBest() {
			return "";
		}

	}

	static class BufferCharLoader extends AbstractStrategy {
		final int threads;
		
		BufferCharLoader(int N, final int threads) {
			super(N, true);
			this.threads = threads;
		}

		@Override
		double load(String path, int bufSize) throws Exception {
			final double[] results = new double[N];

			for (int i = 0; i < N; i++) {
				final long start_time = System.currentTimeMillis();

				if (threads > 1)
					readInChunksMT(path, bufSize);
				else
					readInChunks(path, bufSize);
					
				results[i] = (System.currentTimeMillis() - start_time) / 1000.0;
			}

			return Utils.mean(results);
		}

		void readInChunks(final String path, final int bufSize) throws IOException {
			final File f = new File(path);
			final int size = (int)f.length();

			final FileInputStream is = new FileInputStream(path);

			final byte[] data = new byte[bufSize];
			int readCount = 0;
			int totalLines = 0;

			try {
				while (readCount < size) {
					int n = is.read(data);
					final List<byte[]> lines = extractLines(data, n);
					readCount += data.length;
					totalLines += lines.size(); 
				}
			}
			finally {
				is.close();
			}

			System.out.println("totalLines: "+totalLines);

		}

		private List<byte[]> extractLines(final byte[] data, final int length) {
			final List<byte[]> lines = new ArrayList<byte[]>();

			int startIdx = 0;

			for (int idx = 0; idx < length; idx++) {
				final byte b = data[idx];
				//TODO don't we need to take care of '\r\n' like BufferedReader does ?
				if (b == '\r' || b == '\n') { // new line
					if (startIdx < idx) {
						final byte[] _line = Arrays.copyOfRange(data, startIdx, idx);
						lines.add(_line);
					}
					startIdx = idx+1;
				}
			}
			return lines;
		}

		void readInChunksMT(final String path, final int bufSize) throws IOException, InterruptedException, ExecutionException {
			final File f = new File(path);
			final int size = (int)f.length();
			final int numChunks = size / bufSize;

			final FileInputStream is = new FileInputStream(path);

			final byte[] data = new byte[bufSize];
			int readCount = 0;
			int totalLines = 0;

			final ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
			List<Future<ChunkParser.ParsedResult>> results = new Vector<Future<ChunkParser.ParsedResult>>(numChunks);

			try {
				int id = 0;
				int off = 0;
				while (readCount < size) {
					// make sure each chunk contains complete lines
					int n = is.read(data, off, bufSize - off);
					int total = off + n;
					
					// find the last '\n' in data
					int len = total;
					while (data[len-1] != '\n') len--;
					// data[len] = 1st char after '\n'

					// only copy a complete block
					final byte[] chunk = Arrays.copyOf(data, len);
					
					// copy the incomplete line to the start of data
					off = 0;
					if (len < total) {
						System.arraycopy(data, len, data, 0, total-len);
						off = total - len;
					}

					final Future<ChunkParser.ParsedResult> contentFuture = pool.submit(new ChunkParser(id++, chunk));
					results.add(contentFuture);
					
					readCount += n;
				}

				//TODO because we first read all chunks into memory before starting the next step (processing and sending to tsdb)
				// we will be storing all file bytes into memory in separate chunks of bytes
				// (fix 1) should help reduce the total memory used by the processed data points
				for (Future<ChunkParser.ParsedResult> contentFuture : results) {
					final ChunkParser.ParsedResult res = contentFuture.get();
					totalLines += res.size();
				}
			}
			finally {
				pool.shutdownNow();
				is.close();
			}

			System.out.printf("num chunks: %d, totalLines: %d\n", numChunks, totalLines);
		}

		@Override
		public String toString() {
			return "Buffer Char Loader" + (threads > 0 ? " with "+threads+" threads":"");
		}
	}

	public static class MetricTags {
		final int metric;
		final List<Integer> tagks;
		final List<Integer> tagvs;

		public MetricTags(final int metric) {
			this.metric = metric;
			tagks = new ArrayList<Integer>();
			tagvs = new ArrayList<Integer>();
		}

		@Override
		public int hashCode() {
			// TODO Auto-generated method stub
			return Objects.hash(metric, tagks, tagvs);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) return true;
			if (obj == null || !(obj instanceof MetricTags)) return false;

			MetricTags mts = (MetricTags) obj;

			return metric == mts.metric && tagks.equals(mts.tagks) && tagvs.equals(mts.tagvs);
		}

		public void put(final int tagk, final int tagv) {

			final int tagkIdx = tagks.indexOf(tagk);
			if (tagkIdx != -1) {
				if (tagvs.get(tagkIdx) == tagv) {
					return; // tagk=tagv already present in tags
				}
				//TODO make the error more explicative
				throw new IllegalArgumentException("duplicate tag");
			}

			tagks.add(tagk);
			tagvs.add(tagv);
		}
	}

	public static class TimeValue { // (2+8+4) = 14 bytes
		public static final int SIZE = 14; // in bytes

		private final short mtsIdx;
		public final long timestamp;
		private final int ivalue;

		public boolean isFloat() {
			return mtsIdx < 0;
		}

		public String getValueString() {
			if (isFloat())
				return String.valueOf(Float.intBitsToFloat(ivalue));
			else
				return String.valueOf(ivalue);
		}

		public int getMtsIndex() {
			return mtsIdx;
		}

		public static TimeValue fromByteArray(final byte[] bytes, final int pointId) {
			int cur = pointId* TimeValue.SIZE;
			final short mtsIdx = ByteUtils.getShort(bytes, cur); cur+= 2;
			final long timestamp = ByteUtils.getLong(bytes, cur); cur+= 8;
			final int ivalue = ByteUtils.getInt(bytes, cur); cur+= 4;

			return new TimeValue(mtsIdx, timestamp, ivalue);
		}

		public static void toByteArray(final byte[] bytes, final int pointId, final TimeValue dp) {
			int cur = pointId * TimeValue.SIZE;
			System.arraycopy(ByteUtils.fromShort(dp.mtsIdx), 0, bytes, cur, 2); cur+= 2;
			System.arraycopy(ByteUtils.fromLong(dp.timestamp), 0, bytes, cur, 8); cur+= 8;
			System.arraycopy(ByteUtils.fromInt(dp.ivalue), 0, bytes, cur, 4); cur+= 4;
		}

		public TimeValue(final int mtsIdx, final long timestamp, final String value) {
			this.timestamp = timestamp;

			boolean isfloat = !Utils.looksLikeInteger(value);
			if (isfloat) {
				float fval = Float.parseFloat(value);
				ivalue = Float.floatToRawIntBits(fval);
				this.mtsIdx = (short) -mtsIdx;
			} else {
				ivalue = Integer.parseInt(value);
				this.mtsIdx = (short) mtsIdx;
			}
		}

		public TimeValue(final short mtsIdx, final long timestamp, final int ivalue) {
			this.mtsIdx = mtsIdx;
			this.timestamp = timestamp;
			this.ivalue = ivalue;
		}
	}


}
