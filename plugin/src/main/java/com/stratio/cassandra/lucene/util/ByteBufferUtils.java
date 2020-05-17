package com.stratio.cassandra.lucene.util;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

/** Utility class with some {@link ByteBuffer} transformation utilities.
 *
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 * @author Artem Martynenko artem7mag@gmai.com
 **/
public class ByteBufferUtils {


    /** Returns the specified {@link ByteBuffer} as a byte array.
     *
     * @param bb a {@link ByteBuffer} to be converted to a byte array
     * @return the byte array representation of `bb`
     */
    public static byte[] asArray(ByteBuffer bb){
        ByteBuffer duplicate = bb.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }


    /** Returns `true` if the specified {@link ByteBuffer} is empty, `false` otherwise.
     *
     * @param byteBuffer the byte buffer
     * @return `true` if the specified {@link ByteBuffer} is empty, `false` otherwise.
     */
    public static boolean isEmpty(ByteBuffer byteBuffer){
        return byteBuffer.remaining() == 0;
    }



    /** Returns the {@link ByteBuffer}s contained in the specified byte buffer according to the specified
     * type.
     *
     * @param byteBuffer the byte buffer to be split
     * @param type       the  type of the byte buffer
     * @return the byte buffers contained in `byteBuffer` according to `type`
     */
    public static ByteBuffer[] split(ByteBuffer byteBuffer, AbstractType<?> type){
        if(type instanceof CompositeType){
            return ((CompositeType) type).split(byteBuffer);
        }else {
            return new ByteBuffer[]{byteBuffer};
        }
    }


    /** Returns the hexadecimal {@link String} representation of the specified {@link ByteBuffer}.
     *
     * @param byteBuffer a {@link ByteBuffer}
     * @return the hexadecimal `string` representation of `byteBuffer`
     */
    public static String toHex(ByteBuffer byteBuffer){
        return byteBuffer != null ? ByteBufferUtil.bytesToHex(byteBuffer) : null;
    }




    /** Returns the hexadecimal {@link String} representation of the specified {@link BytesRef}.
     *
     * @param bytesRef a {@link BytesRef}
     * @return the hexadecimal `String` representation of `bytesRef`
     */
    public static String toHex(BytesRef bytesRef){
        return ByteBufferUtil.bytesToHex(byteBuffer(bytesRef));
    }




    /** Returns the hexadecimal {@link String} representation of the specified [[Byte]] array.
     *
     * @param bytes the byte array
     * @return The hexadecimal `String` representation of `bytes`
     */
    public static String toHex(byte... bytes){
        return Hex.bytesToHex(bytes, 0 , bytes.length);
    }



    /** Returns the hexadecimal {@link String} representation of the specified [[Byte]].
     *
     * @param b the byte
     * @return the hexadecimal `String` representation of `b`
     */
    public static String toHex(byte b){
        return Hex.bytesToHex(b);
    }



    /** Returns the {@link BytesRef} representation of the specified {@link ByteBuffer}.
     *
     * @param byteBuffer the byte buffer
     * @return the {@link BytesRef} representation of the byte buffer
     */
    public static BytesRef bytesRef(ByteBuffer byteBuffer){
        return new BytesRef(asArray(byteBuffer));
    }


    /** Returns the {@link ByteBuffer} representation of the specified {@link BytesRef}.
     *
     * @param bytesRef the {@link BytesRef}
     * @return the {@link ByteBuffer} representation of `bytesRef`
     */
    public static ByteBuffer byteBuffer(BytesRef bytesRef){
        return ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }


    /** Returns the {@link ByteBuffer} representation of the specified hex {@link String}.
     *
     * @param hex an hexadecimal representation of a byte array
     * @return the {@link ByteBuffer} representation of `hex`
     */
    public static ByteBuffer byteBuffer(String hex){
        return hex !=null ? ByteBufferUtil.hexToBytes(hex) : null;
    }




    /** Returns a {@link ByteBuffer} representing the specified array of {@link ByteBuffer}s.
     *
     * @param bbs an array of byte buffers
     * @return a {@link ByteBuffer} representing `bbs`
     */
    public static ByteBuffer compose(ByteBuffer... bbs){
        int totalLength = Arrays.stream(bbs).map(ByteBuffer::remaining).reduce((integer, integer2) -> integer  + integer + 2).orElse(2);
        ByteBuffer out = ByteBuffer.allocate(totalLength);
        ByteBufferUtil.writeShortLength(out, bbs.length);
        for(ByteBuffer bb: bbs){
            ByteBufferUtil.writeShortLength(out, bb.remaining());
            out.put(bb.duplicate());
        }
        out.flip();
        return out;
    }


    /** Returns the components of the specified {@link ByteBuffer} created with  {@link this.compose} .
     *
     * @param bb a byte buffer created with [[compose()]]
     * @return the components of `bb`
     */
    public static ByteBuffer[] decompose(ByteBuffer bb){
        ByteBuffer duplicate = bb.duplicate();
        int numComponents = ByteBufferUtil.readShortLength(duplicate);
        return IntStream.range(1, numComponents + 1).boxed().map(integer -> {
            int componentLength = ByteBufferUtil.readShortLength(duplicate);
            return ByteBufferUtil.readBytes(duplicate, componentLength);
        }).toArray(ByteBuffer[]::new);
    }
}
