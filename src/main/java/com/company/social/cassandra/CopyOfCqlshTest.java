package com.company.social.cassandra;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.google.protobuf.InvalidProtocolBufferException;

public class CopyOfCqlshTest
{

    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException
    {

        /* canssandra config */
        Builder builder = Cluster.builder().withCredentials(null, null);
        String[] contactPoints = {"localhost"};

        for (String cp : contactPoints)
            builder.addContactPoint(cp);
        String keySpace = "social";

        Cluster cluster = builder.build();
        final Session session = cluster.connect(keySpace);

        // build buffer
        ApikeyProto.Apikey.Builder b = ApikeyProto.Apikey.newBuilder();
        b.setApikey("apikey");
        b.setMethod("method");
        b.setUrl("url");
        b.setOp(1L);
        boolean bl = b.isInitialized();
        ApikeyProto.Apikey apikey = b.build();
        byte[] bytes = apikey.toByteArray();
        String str1 = bytesToHex(bytes);//show hex
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // test parseFrom
        ApikeyProto.Apikey b2b = ApikeyProto.Apikey.parseFrom(buffer.array());
        b2b = null;

        // insert to C*
        StringBuilder sb = new StringBuilder().append("insert into blob (id, data) values(2, ?)");
        PreparedStatement statement = session.prepare(sb.toString());
        BoundStatement boundStatement = new BoundStatement(statement);
        boundStatement.setBytes(0, buffer);
        session.execute(boundStatement);

        // query from C*
        sb = new StringBuilder().append("select * from blob where id=2");
        statement = session.prepare(sb.toString());
        boundStatement = new BoundStatement(statement);
        ResultSet rs = session.execute(boundStatement);

        for (Row row : rs) {
            ByteBuffer _buffer = row.getBytes("data");
            
            byte[] result = new byte[_buffer.remaining()];
            ByteBuffer _b2 = _buffer.get(result);
            
            str1 = bytesToHex(result);//show hex
            b2b = ApikeyProto.Apikey.parseFrom(result);
            b2b.getApikey();
        }

    }

    public static String bytesToHex(byte[] bytes)
    {
        char[] hexArray = "0123456789ABCDEF".toCharArray();
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    class Blob
    {

    }

    public static Blob toEntity(Row r)
    {
        r.getLong("id");
        r.getBytes("data");
        return null;
    }
}
