import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

/**
* Created by hakim on 12/5/14.
*/
class ChunkParser implements Callable<ChunkParser.ParsedResult> {
    byte[] data;
    final int id;
    final List<Integer> start; // index of first byte of each line
    final List<Integer> length; // length in bytes of each line, '\n' excluded

    final List<SpeedTest.MetricTags> metricTags = new ArrayList<SpeedTest.MetricTags>();
    final List<MtsValue> mtsValues = new ArrayList<MtsValue>();

    ChunkParser(final int id, final byte[] data) {
        this.data = data;
        this.id = id;
        this.start = new ArrayList<Integer>();
        this.length = new ArrayList<Integer>();
    }

    @Override
    public ParsedResult call() throws Exception {
        int startIdx = 0;
        int numlines = 0;

        for (int idx = 0; idx < data.length; idx++) {
            final byte b = data[idx];
            //TODO don't we need to take care of '\r\n' like BufferedReader does ?
            if (b == '\r' || b == '\n') { // new line
                if (startIdx < idx) {
                    start.clear();
                    length.clear();

                    splitLine(startIdx, idx);
                    final SpeedTest.TimeValue dp = processLine();
                    //TODO we don't need to create a TimeValue object before encoding it
//                    SpeedTest.TimeValue.toByteArray(data, numlines, dp);

                    numlines++;
                }
                startIdx = idx+1;
            }
        }

        // create a new, smaller, byte[] to reduce memory consumption
        data = Arrays.copyOf(data, numlines * SpeedTest.TimeValue.SIZE);

        return new ParsedResult(id, data, metricTags, mtsValues, numlines);
    }

    private void splitLine(final int from, final int to) {
        int start = from; // starting index in bytes of the current substring.

        for (int pos = from; pos < to; pos++) {
            if (data[pos] == (byte) SpeedTest.SEPARATOR_CHAR) {
                this.start.add(start);
                this.length.add(pos - start);
                start = pos + 1;
            }
        }

        this.start.add(start);
        this.length.add(to - start);
    }

    private SpeedTest.TimeValue processLine() {
        if (length.get(0) <= 0) {
            throw new RuntimeException("invalid metric");
        }

        final SpeedTest.MetricTags mts = new SpeedTest.MetricTags(addMtsValue(start.get(0), length.get(0)));

		//we need to parse long from string anyway, so we can encode it in a byte[]
        long timestamp = Utils.parseLong(data, start.get(1), length.get(1));
        if (timestamp <= 0) {
            throw new RuntimeException("invalid timestamp: " + timestamp);
        }

        if (length.get(2) <= 0) {
            throw new RuntimeException("invalid value");
        }
        //TODO encode the value as byte[] straight from byte[] without converting to a String first
        final String value = Utils.bytes2str(data, start.get(2), length.get(2));

        for (int i = 3; i < start.size(); i++) {
            parseTag(mts, start.get(i), length.get(i));
        }

        int mtsId = metricTags.indexOf(mts);
        if (mtsId < 0) {
            mtsId = metricTags.size();
            metricTags.add(mts);
        }

//        return new SpeedTest.TimeValue(mtsId, timestamp, value);
        return null;
    }

    private void parseTag(final SpeedTest.MetricTags mts, final int offset, final int length) {
        int equalIdx = 0;
        for (; equalIdx < length; equalIdx++) {
            if (data[offset + equalIdx] == (byte)'=') {
                break;
            }
        }
        if (equalIdx <= 0 || equalIdx == length) {
            throw new IllegalArgumentException("invalid tag: "+Utils.bytes2str(data, offset, length));
        }
        //TODO check if there are more than one '=' char
		final int tagk = addMtsValue(offset, equalIdx - 1);
		final int tagv = addMtsValue(offset + equalIdx + 1, length - equalIdx - 1);
		mts.put(tagk, tagv);
    }

    private int addMtsValue(final int offset, final int length) {
        mainloop:
        for (int idx = 0; idx < mtsValues.size(); idx++) {
            final MtsValue value = mtsValues.get(idx);
            //TODO move the comparison in MtsValue.equals() ?
            if (length != value.length) continue;

            for (int i = 0; i < length; i++)
                if (data[i + offset] != data[i + value.offset]) continue mainloop;
            //TODO we could probably use 'short' here
            return idx;
        }

        mtsValues.add(new MtsValue(offset, length));
        return mtsValues.size() - 1;
    }

	public static class ParsedResult {
		final int id;
		final byte[] data;
		final int numPoints;

		final List<SpeedTest.MetricTags> metricTags;
		final List<MtsValue> mtsValues;

		public int size() {
			return numPoints;
		}

		public ParsedResult(final int id, final byte[] data, final List<SpeedTest.MetricTags> metricTags, final List<MtsValue> mtsValues, final int numPoints) {
			this.id = id;
			this.data = data;
			this.metricTags = metricTags;
			this.mtsValues = mtsValues;
			this.numPoints = numPoints;
		}

		/**
		 * Import a datapoint into TSDB using CachedBatches. Applies concatenation+repetition offset and generates duplicates if necessary
		 */
		public void import2tsdb(/*final TSDB tsdb,*/) {
			for (int point = 0; point < numPoints; point++) {
				final SpeedTest.TimeValue dp = SpeedTest.TimeValue.fromByteArray(data, point);

				final SpeedTest.MetricTags mts = metricTags.get(dp.getMtsIndex());

//				CachedBatches.addPoint(tsdb, mts.metric, dp.timestamp, dp.getValueString(), mts.getTags());
			}
		}
	}

	private static class MtsValue {
		final int offset;
		final int length;

		public MtsValue(final int offset, final int length) {
			this.offset = offset;
			this.length = length;
		}
	}
}
