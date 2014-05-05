/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.pig;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;


import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.hadoop.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.UUIDGen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Cassandra
 */
public abstract class AbstractCassandraStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata
{
    protected enum MarshallerType { COMPARATOR, DEFAULT_VALIDATOR, KEY_VALIDATOR, SUBCOMPARATOR };

    // system environment variables that can be set to configure connection info:
    // alternatively, Hadoop JobConf variables can be set using keys from ConfigHelper
    public final static String PIG_INPUT_RPC_PORT = "PIG_INPUT_RPC_PORT";
    public final static String PIG_INPUT_INITIAL_ADDRESS = "PIG_INPUT_INITIAL_ADDRESS";
    public final static String PIG_INPUT_PARTITIONER = "PIG_INPUT_PARTITIONER";
    public final static String PIG_OUTPUT_RPC_PORT = "PIG_OUTPUT_RPC_PORT";
    public final static String PIG_OUTPUT_INITIAL_ADDRESS = "PIG_OUTPUT_INITIAL_ADDRESS";
    public final static String PIG_OUTPUT_PARTITIONER = "PIG_OUTPUT_PARTITIONER";
    public final static String PIG_RPC_PORT = "PIG_RPC_PORT";
    public final static String PIG_INITIAL_ADDRESS = "PIG_INITIAL_ADDRESS";
    public final static String PIG_PARTITIONER = "PIG_PARTITIONER";
    public final static String PIG_INPUT_FORMAT = "PIG_INPUT_FORMAT";
    public final static String PIG_OUTPUT_FORMAT = "PIG_OUTPUT_FORMAT";
    public final static String PIG_INPUT_SPLIT_SIZE = "PIG_INPUT_SPLIT_SIZE";

    protected String DEFAULT_INPUT_FORMAT;
    protected String DEFAULT_OUTPUT_FORMAT;

    public final static String PARTITION_FILTER_SIGNATURE = "cassandra.partition.filter";

    protected static final Logger logger = LoggerFactory.getLogger(AbstractCassandraStorage.class);

    protected String username;
    protected String password;
    protected String keyspace;
    protected String column_family;
    protected String loadSignature;
    protected String storeSignature;

    protected Configuration conf;
    protected String inputFormatClass;
    protected String outputFormatClass;
    protected int splitSize = 64 * 1024;
    protected String partitionerClass;
    protected boolean usePartitionFilter = false; 

    public AbstractCassandraStorage()
    {
        super();
    }

    /** Deconstructs a composite type to a Tuple. */
    protected Tuple composeComposite(AbstractCompositeType comparator, ByteBuffer name) throws IOException
    {
        List<CompositeComponent> result = comparator.deconstruct(name);
        Tuple t = TupleFactory.getInstance().newTuple(result.size());
        for (int i=0; i<result.size(); i++)
            setTupleValue(t, i, result.get(i).comparator.compose(result.get(i).value));

        return t;
    }

    /** convert a column to a tuple */
    protected Tuple columnToTuple(IColumn col, CfDef cfDef, AbstractType comparator) throws IOException
    {
        Tuple pair = TupleFactory.getInstance().newTuple(2);

        // name
        if(comparator instanceof AbstractCompositeType)
            setTupleValue(pair, 0, composeComposite((AbstractCompositeType)comparator,col.name()));
        else
            setTupleValue(pair, 0, comparator.compose(col.name()));

        // value
        if (col instanceof Column)
        {
            // standard
            Map<ByteBuffer,AbstractType> validators = getValidatorMap(cfDef);
            if (validators.get(col.name()) == null)
            {
                Map<MarshallerType, AbstractType> marshallers = getDefaultMarshallers(cfDef);
                setTupleValue(pair, 1, marshallers.get(MarshallerType.DEFAULT_VALIDATOR).compose(col.value()));
            }
            else
                setTupleValue(pair, 1, validators.get(col.name()).compose(col.value()));
            return pair;
        }
        else
        {
            // super
            ArrayList<Tuple> subcols = new ArrayList<Tuple>();
            for (IColumn subcol : col.getSubColumns())
                subcols.add(columnToTuple(subcol, cfDef, parseType(cfDef.getSubcomparator_type())));

            pair.set(1, new DefaultDataBag(subcols));
        }
        return pair;
    }

    /** set the value to the position of the tuple */
    protected void setTupleValue(Tuple pair, int position, Object value) throws ExecException
    {
       if (value instanceof BigInteger)
           pair.set(position, ((BigInteger) value).intValue());
       else if (value instanceof ByteBuffer)
           pair.set(position, new DataByteArray(ByteBufferUtil.getArray((ByteBuffer) value)));
       else if (value instanceof UUID)
           pair.set(position, new DataByteArray(UUIDGen.decompose((java.util.UUID) value)));
       else if (value instanceof Date)
           pair.set(position, DateType.instance.decompose((Date) value).getLong());
       else
           pair.set(position, value);
    }

    /** get the columnfamily definition for the signature */
    protected CfDef getCfDef(String signature)
    {
        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(AbstractCassandraStorage.class);
        return cfdefFromString(property.getProperty(signature));
    }

    /** construct a map to store the mashaller type to cassandra data type mapping */
    protected Map<MarshallerType, AbstractType> getDefaultMarshallers(CfDef cfDef) throws IOException
    {
        Map<MarshallerType, AbstractType> marshallers = new EnumMap<MarshallerType, AbstractType>(MarshallerType.class);
        AbstractType comparator;
        AbstractType subcomparator;
        AbstractType default_validator;
        AbstractType key_validator;

        comparator = parseType(cfDef.getComparator_type());
        subcomparator = parseType(cfDef.getSubcomparator_type());
        default_validator = parseType(cfDef.getDefault_validation_class());
        key_validator = parseType(cfDef.getKey_validation_class());

        marshallers.put(MarshallerType.COMPARATOR, comparator);
        marshallers.put(MarshallerType.DEFAULT_VALIDATOR, default_validator);
        marshallers.put(MarshallerType.KEY_VALIDATOR, key_validator);
        marshallers.put(MarshallerType.SUBCOMPARATOR, subcomparator);
        return marshallers;
    }

    /** get the validators */
    protected Map<ByteBuffer, AbstractType> getValidatorMap(CfDef cfDef) throws IOException
    {
        Map<ByteBuffer, AbstractType> validators = new HashMap<ByteBuffer, AbstractType>();
        for (ColumnDef cd : cfDef.getColumn_metadata())
        {
            if (cd.getValidation_class() != null && !cd.getValidation_class().isEmpty())
            {
                AbstractType validator = null;
                try
                {
                    validator = TypeParser.parse(cd.getValidation_class());
                    validators.put(cd.name, validator);
                }
                catch (ConfigurationException e)
                {
                    throw new IOException(e);
                }
                catch (SyntaxException e)
                {
                    throw new IOException(e);
                }
            }
        }
        return validators;
    }

    /** parse the string to a cassandra data type */
    protected AbstractType parseType(String type) throws IOException
    {
        try
        {
            // always treat counters like longs, specifically CCT.compose is not what we need
            if (type != null && type.equals("org.apache.cassandra.db.marshal.CounterColumnType"))
                    return LongType.instance;
            return TypeParser.parse(type);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }
        catch (SyntaxException e)
        {
            throw new IOException(e);
        }
    }

    @Override
    public InputFormat getInputFormat()
    {
        try
        {
            return FBUtilities.construct(inputFormatClass, "inputformat");
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** decompose the query to store the parameters in a map */
    public static Map<String, String> getQueryMap(String query) throws UnsupportedEncodingException 
    {
        String[] params = query.split("&");
        Map<String, String> map = new HashMap<String, String>();
        for (String param : params)
        {
            String[] keyValue = param.split("=");
            map.put(keyValue[0], URLDecoder.decode(keyValue[1],"UTF-8"));
        }
        return map;
    }

    /** set hadoop cassandra connection settings */
    protected void setConnectionInformation() throws IOException
    {
        if (System.getenv(PIG_RPC_PORT) != null)
        {
            ConfigHelper.setInputRpcPort(conf, System.getenv(PIG_RPC_PORT));
            ConfigHelper.setOutputRpcPort(conf, System.getenv(PIG_RPC_PORT));
        }

        if (System.getenv(PIG_INPUT_RPC_PORT) != null)
            ConfigHelper.setInputRpcPort(conf, System.getenv(PIG_INPUT_RPC_PORT));
        if (System.getenv(PIG_OUTPUT_RPC_PORT) != null)
            ConfigHelper.setOutputRpcPort(conf, System.getenv(PIG_OUTPUT_RPC_PORT));

        if (System.getenv(PIG_INITIAL_ADDRESS) != null)
        {
            ConfigHelper.setInputInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
            ConfigHelper.setOutputInitialAddress(conf, System.getenv(PIG_INITIAL_ADDRESS));
        }
        if (System.getenv(PIG_INPUT_INITIAL_ADDRESS) != null)
            ConfigHelper.setInputInitialAddress(conf, System.getenv(PIG_INPUT_INITIAL_ADDRESS));
        if (System.getenv(PIG_OUTPUT_INITIAL_ADDRESS) != null)
            ConfigHelper.setOutputInitialAddress(conf, System.getenv(PIG_OUTPUT_INITIAL_ADDRESS));

        if (System.getenv(PIG_PARTITIONER) != null)
        {
            ConfigHelper.setInputPartitioner(conf, System.getenv(PIG_PARTITIONER));
            ConfigHelper.setOutputPartitioner(conf, System.getenv(PIG_PARTITIONER));
        }
        if(System.getenv(PIG_INPUT_PARTITIONER) != null)
            ConfigHelper.setInputPartitioner(conf, System.getenv(PIG_INPUT_PARTITIONER));
        if(System.getenv(PIG_OUTPUT_PARTITIONER) != null)
            ConfigHelper.setOutputPartitioner(conf, System.getenv(PIG_OUTPUT_PARTITIONER));
        if (System.getenv(PIG_INPUT_FORMAT) != null)
            inputFormatClass = getFullyQualifiedClassName(System.getenv(PIG_INPUT_FORMAT));
        else
            inputFormatClass = DEFAULT_INPUT_FORMAT;
        if (System.getenv(PIG_OUTPUT_FORMAT) != null)
            outputFormatClass = getFullyQualifiedClassName(System.getenv(PIG_OUTPUT_FORMAT));
        else
            outputFormatClass = DEFAULT_OUTPUT_FORMAT;
    }

    /** get the full class name */
    protected String getFullyQualifiedClassName(String classname)
    {
        return classname.contains(".") ? classname : "org.apache.cassandra.hadoop." + classname;
    }

    /** get pig type for the cassandra data type*/
    protected byte getPigType(AbstractType type)
    {
        if (type instanceof LongType || type instanceof DateType) // DateType is bad and it should feel bad
            return DataType.LONG;
        else if (type instanceof IntegerType || type instanceof Int32Type) // IntegerType will overflow at 2**31, but is kept for compatibility until pig has a BigInteger
            return DataType.INTEGER;
        else if (type instanceof AsciiType)
            return DataType.CHARARRAY;
        else if (type instanceof UTF8Type)
            return DataType.CHARARRAY;
        else if (type instanceof FloatType)
            return DataType.FLOAT;
        else if (type instanceof DoubleType)
            return DataType.DOUBLE;
        else if (type instanceof AbstractCompositeType )
            return DataType.TUPLE;

        return DataType.BYTEARRAY;
    }

    public ResourceStatistics getStatistics(String location, Job job)
    {
        return null;
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir) throws IOException
    {
        return location;
    }

    @Override
    public void setUDFContextSignature(String signature)
    {
        this.loadSignature = signature;
    }

    /** StoreFunc methods */
    public void setStoreFuncUDFContextSignature(String signature)
    {
        this.storeSignature = signature;
    }

    public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException
    {
        return relativeToAbsolutePath(location, curDir);
    }

    /** output format */
    public OutputFormat getOutputFormat()
    {
        try
        {
            return FBUtilities.construct(outputFormatClass, "outputformat");
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void checkSchema(ResourceSchema schema) throws IOException
    {
        // we don't care about types, they all get casted to ByteBuffers
    }

    /** convert object to ByteBuffer */
    protected ByteBuffer objToBB(Object o)
    {
        if (o == null)
            return (ByteBuffer)o;
        if (o instanceof java.lang.String)
            return ByteBuffer.wrap(new DataByteArray((String)o).get());
        if (o instanceof Integer)
            return Int32Type.instance.decompose((Integer)o);
        if (o instanceof Long)
            return LongType.instance.decompose((Long)o);
        if (o instanceof Float)
            return FloatType.instance.decompose((Float)o);
        if (o instanceof Double)
            return DoubleType.instance.decompose((Double)o);
        if (o instanceof UUID)
            return ByteBuffer.wrap(UUIDGen.decompose((UUID) o));
        if(o instanceof Tuple) {
            List<Object> objects = ((Tuple)o).getAll();
            List<ByteBuffer> serialized = new ArrayList<ByteBuffer>(objects.size());
            int totalLength = 0;
            for(Object sub : objects)
            {
                ByteBuffer buffer = objToBB(sub);
                serialized.add(buffer);
                totalLength += 2 + buffer.remaining() + 1;
            }
            ByteBuffer out = ByteBuffer.allocate(totalLength);
            for (ByteBuffer bb : serialized)
            {
                int length = bb.remaining();
                out.put((byte) ((length >> 8) & 0xFF));
                out.put((byte) (length & 0xFF));
                out.put(bb);
                out.put((byte) 0);
            }
            out.flip();
            return out;
        }

        return ByteBuffer.wrap(((DataByteArray) o).get());
    }

    public void cleanupOnFailure(String failure, Job job)
    {
    }

    /** Methods to get the column family schema from Cassandra */
    protected void initSchema(String signature)
    {
        Properties properties = UDFContext.getUDFContext().getUDFProperties(AbstractCassandraStorage.class);

        // Only get the schema if we haven't already gotten it
        if (!properties.containsKey(signature))
        {
            try
            {
                Cassandra.Client client = ConfigHelper.getClientFromInputAddressList(conf);
                client.set_keyspace(keyspace);

                if (username != null && password != null)
                {
                    Map<String, String> credentials = new HashMap<String, String>(2);
                    credentials.put(IAuthenticator.USERNAME_KEY, username);
                    credentials.put(IAuthenticator.PASSWORD_KEY, password);

                    try
                    {
                        client.login(new AuthenticationRequest(credentials));
                    }
                    catch (AuthenticationException e)
                    {
                        logger.error("Authentication exception: invalid username and/or password");
                        throw new RuntimeException(e);
                    }
                    catch (AuthorizationException e)
                    {
                        throw new AssertionError(e); // never actually throws AuthorizationException.
                    }
                }

                // compose the CfDef for the columfamily
                CfDef cfDef = getCfDef(client);

                if (cfDef != null)
                    properties.setProperty(signature, cfdefToString(cfDef));
                else
                    throw new RuntimeException(String.format("Column family '%s' not found in keyspace '%s'",
                                                             column_family,
                                                             keyspace));
            }
            catch (TException e)
            {
                throw new RuntimeException(e);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException(e);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            catch (UnavailableException e)
            {
                throw new RuntimeException(e);
            }
            catch (TimedOutException e)
            {
                throw new RuntimeException(e);
            }
            catch (SchemaDisagreementException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    /** convert CfDef to string */
    protected static String cfdefToString(CfDef cfDef)
    {
        assert cfDef != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        try
        {
            return Hex.bytesToHex(serializer.serialize(cfDef));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** convert string back to CfDef */
    protected static CfDef cfdefFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
        CfDef cfDef = new CfDef();
        try
        {
            deserializer.deserialize(cfDef, Hex.hexToBytes(st));
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return cfDef;
    }

    /** return the CfDef for the column family */
    protected CfDef getCfDef(Cassandra.Client client)
            throws InvalidRequestException,
                   UnavailableException,
                   TimedOutException,
                   SchemaDisagreementException,
                   TException,
                   CharacterCodingException
    {
        // get CF meta data
        String query = "SELECT type, " +
                       "       comparator," +
                       "       subcomparator," +
                       "       default_validator, " +
                       "       key_validator," +
                       "       key_aliases " +
                       "FROM system.schema_columnfamilies " +
                       "WHERE keyspace_name = '%s' " +
                       "  AND columnfamily_name = '%s' ";

        CqlResult result = client.execute_cql3_query(
                                ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                                Compression.NONE,
                                ConsistencyLevel.ONE);

        if (result == null || result.rows == null || result.rows.isEmpty())
            return null;

        Iterator<CqlRow> iteraRow = result.rows.iterator();
        CfDef cfDef = new CfDef();
        cfDef.keyspace = keyspace;
        cfDef.name = column_family;
        boolean cql3Table = false;
        if (iteraRow.hasNext())
        {
            CqlRow cqlRow = iteraRow.next();

            cfDef.column_type = ByteBufferUtil.string(cqlRow.columns.get(0).value);
            cfDef.comparator_type = ByteBufferUtil.string(cqlRow.columns.get(1).value);
            ByteBuffer subComparator = cqlRow.columns.get(2).value;
            if (subComparator != null)
                cfDef.subcomparator_type = ByteBufferUtil.string(subComparator);
            cfDef.default_validation_class = ByteBufferUtil.string(cqlRow.columns.get(3).value);
            cfDef.key_validation_class = ByteBufferUtil.string(cqlRow.columns.get(4).value);
            List<String> keys = null;
            if (cqlRow.columns.get(5).value != null)
            {
                String keyAliases = ByteBufferUtil.string(cqlRow.columns.get(5).value);
                keys = FBUtilities.fromJsonList(keyAliases);
            }
            // get column meta data
            if (keys != null && keys.size() > 0)
                cql3Table = true;
        }
        cfDef.column_metadata = getColumnMetadata(client, cql3Table);
        return cfDef;
    }

    /** get a list of columns */
    protected abstract List<ColumnDef> getColumnMetadata(Cassandra.Client client, boolean cql3Table)
            throws InvalidRequestException,
            UnavailableException,
            TimedOutException,
            SchemaDisagreementException,
            TException,
            CharacterCodingException;

    /** get column meta data */
    protected List<ColumnDef> getColumnMeta(Cassandra.Client client)
            throws InvalidRequestException,
            UnavailableException,
            TimedOutException,
            SchemaDisagreementException,
            TException,
            CharacterCodingException
    {
        String query = "SELECT column_name, " +
                       "       validator, " +
                       "       index_type " +
                       "FROM system.schema_columns " +
                       "WHERE keyspace_name = '%s' " +
                       "  AND columnfamily_name = '%s'";

        CqlResult result = client.execute_cql3_query(
                                   ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                                   Compression.NONE,
                                   ConsistencyLevel.ONE);

        List<CqlRow> rows = result.rows;
        List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
        if (rows == null || rows.isEmpty())
            return columnDefs;

        Iterator<CqlRow> iterator = rows.iterator();
        while (iterator.hasNext())
        {
            CqlRow row = iterator.next();
            ColumnDef cDef = new ColumnDef();
            cDef.setName(ByteBufferUtil.clone(row.getColumns().get(0).value));
            cDef.validation_class = ByteBufferUtil.string(row.getColumns().get(1).value);
            ByteBuffer indexType = row.getColumns().get(2).value;
            if (indexType != null)
                cDef.index_type = getIndexType(ByteBufferUtil.string(indexType));
            columnDefs.add(cDef);
        }
        return columnDefs;
    }

    /** get keys meta data  */
    protected List<ColumnDef> getKeysMeta(Cassandra.Client client)
            throws InvalidRequestException,
            UnavailableException,
            TimedOutException,
            SchemaDisagreementException,
            TException,
            IOException
    {
        String query = "SELECT key_aliases, " +
                       "       column_aliases, " +
                       "       key_validator, " +
                       "       comparator, " +
                       "       keyspace_name, " +
                       "       value_alias, " +
                       "       default_validator  " +
                       "FROM system.schema_columnfamilies " +
                       "WHERE keyspace_name = '%s'" +
                       "  AND columnfamily_name = '%s' ";

        CqlResult result = client.execute_cql3_query(
                                            ByteBufferUtil.bytes(String.format(query, keyspace, column_family)),
                                            Compression.NONE,
                                            ConsistencyLevel.ONE);

        if (result == null || result.rows == null || result.rows.isEmpty())
            return null;

        List<CqlRow> rows = result.rows;
        Iterator<CqlRow> iteraRow = rows.iterator();
        List<ColumnDef> keys = new ArrayList<ColumnDef>();
        if (iteraRow.hasNext())
        {
            CqlRow cqlRow = iteraRow.next();
            String name = ByteBufferUtil.string(cqlRow.columns.get(4).value);
            logger.debug("Found ksDef name: {}", name);
            String keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(0).getValue()));

            logger.debug("partition keys: {}", keyString);
            List<String> keyNames = FBUtilities.fromJsonList(keyString);
 
            Iterator<String> iterator = keyNames.iterator();
            while (iterator.hasNext())
            {
                ColumnDef cDef = new ColumnDef();
                cDef.name = ByteBufferUtil.bytes(iterator.next());
                keys.add(cDef);
            }

            keyString = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(1).getValue()));

            logger.debug("cluster keys: {}", keyString);
            keyNames = FBUtilities.fromJsonList(keyString);

            iterator = keyNames.iterator();
            while (iterator.hasNext())
            {
                ColumnDef cDef = new ColumnDef();
                cDef.name = ByteBufferUtil.bytes(iterator.next());
                keys.add(cDef);
            }

            String validator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(2).getValue()));
            logger.debug("row key validator: {}", validator);
            AbstractType<?> keyValidator = parseType(validator);

            Iterator<ColumnDef> keyItera = keys.iterator();
            if (keyValidator instanceof CompositeType)
            {
                Iterator<AbstractType<?>> typeItera = ((CompositeType) keyValidator).types.iterator();
                while (typeItera.hasNext())
                    keyItera.next().validation_class = typeItera.next().toString();
            }
            else
                keyItera.next().validation_class = keyValidator.toString();

            validator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(3).getValue()));
            logger.debug("cluster key validator: {}", validator);

            if (keyItera.hasNext() && validator != null && !validator.isEmpty())
            {
                AbstractType<?> clusterKeyValidator = parseType(validator);

                if (clusterKeyValidator instanceof CompositeType)
                {
                    Iterator<AbstractType<?>> typeItera = ((CompositeType) clusterKeyValidator).types.iterator();
                    while (keyItera.hasNext())
                        keyItera.next().validation_class = typeItera.next().toString();
                }
                else
                    keyItera.next().validation_class = clusterKeyValidator.toString();
            }

            // compact value_alias column
            if (cqlRow.columns.get(5).value != null)
            {
                try
                {
                    String compactValidator = ByteBufferUtil.string(ByteBuffer.wrap(cqlRow.columns.get(6).getValue()));
                    logger.debug("default validator: {}", compactValidator);
                    AbstractType<?> defaultValidator = parseType(compactValidator);

                    ColumnDef cDef = new ColumnDef();
                    cDef.name = cqlRow.columns.get(5).value;
                    cDef.validation_class = defaultValidator.toString();
                    keys.add(cDef);
                }
                catch (Exception e)
                {
                    // no compact column at value_alias
                }
            }

        }
        return keys;
    }

    /** get index type from string */
    protected IndexType getIndexType(String type)
    {
        type = type.toLowerCase();
        if ("keys".equals(type))
            return IndexType.KEYS;
        else if("custom".equals(type))
            return IndexType.CUSTOM;
        else if("composites".equals(type))
            return IndexType.COMPOSITES;
        else
            return null;
    }

    /** return partition keys */
    public String[] getPartitionKeys(String location, Job job)
    {
        if (!usePartitionFilter)
            return null;
        List<ColumnDef> indexes = getIndexes();
        String[] partitionKeys = new String[indexes.size()];
        for (int i = 0; i < indexes.size(); i++)
        {
            partitionKeys[i] = new String(indexes.get(i).getName());
        }
        return partitionKeys;
    }

    /** get a list of columns with defined index*/
    protected List<ColumnDef> getIndexes()
    {
        CfDef cfdef = getCfDef(loadSignature);
        List<ColumnDef> indexes = new ArrayList<ColumnDef>();
        for (ColumnDef cdef : cfdef.column_metadata)
        {
            if (cdef.index_type != null)
                indexes.add(cdef);
        }
        return indexes;
    }
}

