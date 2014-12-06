/**
 * Extracted from org.hbase.async.Bytes
 */
public class ByteUtils {

    public static short getShort(byte[] b, int offset) {
        return (short)(b[offset] << 8 | b[offset + 1] & 255);
    }

    public static void setShort(byte[] b, short n) {
        setShort(b, n, 0);
    }

    public static void setShort(byte[] b, short n, int offset) {
        b[offset + 0] = (byte)(n >>> 8);
        b[offset + 1] = (byte)(n >>> 0);
    }

    public static byte[] fromShort(short n) {
        byte[] b = new byte[2];
        setShort(b, n);
        return b;
    }

    public static long getLong(byte[] b, int offset) {
        return ((long)b[offset + 0] & 255L) << 56 | ((long)b[offset + 1] & 255L) << 48 | ((long)b[offset + 2] & 255L) << 40 | ((long)b[offset + 3] & 255L) << 32 | ((long)b[offset + 4] & 255L) << 24 | ((long)b[offset + 5] & 255L) << 16 | ((long)b[offset + 6] & 255L) << 8 | ((long)b[offset + 7] & 255L) << 0;
    }

    public static void setLong(byte[] b, long n) {
        setLong(b, n, 0);
    }

    public static void setLong(byte[] b, long n, int offset) {
        b[offset + 0] = (byte)((int)(n >>> 56));
        b[offset + 1] = (byte)((int)(n >>> 48));
        b[offset + 2] = (byte)((int)(n >>> 40));
        b[offset + 3] = (byte)((int)(n >>> 32));
        b[offset + 4] = (byte)((int)(n >>> 24));
        b[offset + 5] = (byte)((int)(n >>> 16));
        b[offset + 6] = (byte)((int)(n >>> 8));
        b[offset + 7] = (byte)((int)(n >>> 0));
    }

    public static byte[] fromLong(long n) {
        byte[] b = new byte[8];
        setLong(b, n);
        return b;
    }

    public static int getInt(byte[] b, int offset) {
        return (b[offset + 0] & 255) << 24 | (b[offset + 1] & 255) << 16 | (b[offset + 2] & 255) << 8 | (b[offset + 3] & 255) << 0;
    }

    public static void setInt(byte[] b, int n) {
        setInt(b, n, 0);
    }

    public static void setInt(byte[] b, int n, int offset) {
        b[offset + 0] = (byte)(n >>> 24);
        b[offset + 1] = (byte)(n >>> 16);
        b[offset + 2] = (byte)(n >>> 8);
        b[offset + 3] = (byte)(n >>> 0);
    }

    public static byte[] fromInt(int n) {
        byte[] b = new byte[4];
        setInt(b, n);
        return b;
    }
}
