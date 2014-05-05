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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.metadata.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.thrift.ThriftValidation.validateColumnFamily;

public class AlterTableStatement extends SchemaAlteringStatement
{
    public static enum Type
    {
        ADD, ALTER, DROP, OPTS, RENAME
    }

    public final Type oType;
    public final CQL3Type validator;
    public final ColumnIdentifier columnName;
    private final CFPropDefs cfProps;
    private final Map<ColumnIdentifier, ColumnIdentifier> renames;

    public AlterTableStatement(CFName name, Type type, ColumnIdentifier columnName, CQL3Type validator, CFPropDefs cfProps, Map<ColumnIdentifier, ColumnIdentifier> renames)
    {
        super(name);
        this.oType = type;
        this.columnName = columnName;
        this.validator = validator; // used only for ADD/ALTER commands
        this.cfProps = cfProps;
        this.renames = renames;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void announceMigration() throws RequestValidationException
    {
        CFMetaData meta = validateColumnFamily(keyspace(), columnFamily());
        CFMetaData cfm = meta.clone();

        CFDefinition cfDef = meta.getCfDef();
        CFDefinition.Name name = columnName == null ? null : cfDef.get(columnName);
        String logValue = "";
        String target, exists;
        		
        switch (oType)
        {
            case ADD:
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot add new column to a compact CF");
                if (name != null)
                {
                    switch (name.kind)
                    {
                        case KEY_ALIAS:
                        case COLUMN_ALIAS:
                            throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with a PRIMARY KEY part", columnName));
                        case COLUMN_METADATA:
							throw new InvalidRequestException(String.format("Invalid column name %s because it conflicts with an existing column", columnName));
                    }
                }
                
                target = keyspace() + "." + columnFamily() + "." + columnName.toString();
            	exists = MetadataRegistry.instance.query(target, Metadata.AlterColumnFamily_Drop_Tag);
            	if(exists != null){
            		Metadata.mutate(MetadataRegistry.instance.drop(target));
            	}
                
				// The original functionality
                AbstractType<?> type = validator.getType();
                if (type instanceof CollectionType)
                {
                    if (!cfDef.isComposite)
                        throw new InvalidRequestException("Cannot use collection types with non-composite PRIMARY KEY");

                    Map<ByteBuffer, CollectionType> collections = cfDef.hasCollections
                                                                ? new HashMap<ByteBuffer, CollectionType>(cfDef.getCollectionType().defined)
                                                                : new HashMap<ByteBuffer, CollectionType>();

                    collections.put(columnName.key, (CollectionType)type);
                    ColumnToCollectionType newColType = ColumnToCollectionType.getInstance(collections);
                    List<AbstractType<?>> ctypes = new ArrayList<AbstractType<?>>(((CompositeType)cfm.comparator).types);
                    if (cfDef.hasCollections)
                        ctypes.set(ctypes.size() - 1, newColType);
                    else
                        ctypes.add(newColType);
                    cfm.comparator = CompositeType.getInstance(ctypes);
                }

                Integer componentIndex = cfDef.isComposite
                                       ? ((CompositeType)meta.comparator).types.size() - (cfDef.hasCollections ? 2 : 1)
                                       : null;
                cfm.addColumnDefinition(new ColumnDefinition(columnName.key,
                                                             type,
                                                             null,
                                                             null,
                                                             null,
                                                             componentIndex));
                
                logValue = "column_name=" + columnName.toString() + "," + "cql_type=" + type.asCQL3Type().toString();
                MetadataLog.announce(keyspace() + "." + columnFamily(), Metadata.AlterColumnFamily_Add_Tag, clientState, logValue);
                break;

            case ALTER:
            	
            	AbstractType<?> oldType = cfm.getDefaultValidator();
            	
                if (name == null)
                    throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    	oldType = cfm.getKeyValidator();
                        AbstractType<?> newType = validator.getType();
                        if (newType instanceof CounterColumnType)
                            throw new InvalidRequestException(String.format("counter type is not supported for PRIMARY KEY part %s", columnName));
                        if (cfDef.hasCompositeKey)
                        {
                            List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(((CompositeType) cfm.getKeyValidator()).types);
                            newTypes.set(name.position, newType);
                            cfm.keyValidator(CompositeType.getInstance(newTypes));
                        }
                        else
                        {
                            cfm.keyValidator(newType);
                        }
                        break;
                    case COLUMN_ALIAS:
                        assert cfDef.isComposite;
                        oldType = ((CompositeType) cfm.comparator).types.get(name.position);
                        List<AbstractType<?>> newTypes = new ArrayList<AbstractType<?>>(((CompositeType) cfm.comparator).types);
                        newTypes.set(name.position, validator.getType());
                        cfm.comparator = CompositeType.getInstance(newTypes);
                        break;
                    case VALUE_ALIAS:
                    	oldType = cfm.getDefaultValidator();
                        // See below
                        if (!validator.getType().isCompatibleWith(cfm.getDefaultValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           cfm.getDefaultValidator().asCQL3Type(),
                                                                           validator));
                        cfm.defaultValidator(validator.getType());
                        break;
                    case COLUMN_METADATA:
                        ColumnDefinition column = cfm.getColumnDefinition(columnName.key);
                        // Thrift allows to change a column validator so CFMetaData.validateCompatility will let it slide
                        // if we change to an incompatible type (contrarily to the comparator case). But we don't want to
                        // allow it for CQL3 (see #5882) so validating it explicitly here
                        if (!validator.getType().isCompatibleWith(column.getValidator()))
                            throw new ConfigurationException(String.format("Cannot change %s from type %s to type %s: types are incompatible.",
                                                                           columnName,
                                                                           column.getValidator().asCQL3Type(),
                                                                           validator));

                        oldType = column.getValidator();
                        
                        column.setValidator(validator.getType());
                        cfm.addColumnDefinition(column);
                        break;
                }
                
                logValue = "Old: " + "column_name=" + columnName.toString() + "," + "cql_type=" + oldType.asCQL3Type().toString() + ";" +
                		   "New: " + "column_name=" + columnName.toString() + "," + "cql_type=" + validator.getType().asCQL3Type().toString();
                
                MetadataLog.announce(keyspace() + "." + columnFamily(), Metadata.AlterColumnFamily_Alter_Tag, clientState, logValue);
                
                break;

            case DROP:
            	ColumnDefinition toDelete = null;
            	boolean removeColDef = false;
            	
                if (cfDef.isCompact)
                    throw new InvalidRequestException("Cannot drop columns from a compact CF");
                if (!cfDef.isComposite)
                	throw new InvalidRequestException("Cannot drop columns from a non-CQL3 CF");
                if (name == null){
                	// log permanent deletion deletion in metadata
                	target = keyspace() + "." + columnFamily() + "." + columnName.toString();
                	exists = MetadataRegistry.instance.query(target, Metadata.AlterColumnFamily_Drop_Tag);
                	if(exists != null){
                		String client = (clientState == null)? "" : clientState.getUser().getName();
                		Metadata.mutate(MetadataLog.add(target, FBUtilities.timestampMicros(), client,
                				Metadata.AlterColumnFamily_Drop_Tag, "Permanent Drop", ""));
//                		// register the column in the CFMetadata
//                		Metadata.mutate(MetadataRegistry.instance.add(keyspace() + "." + columnFamily() + "." + columnName.toString(),
//                     		   Metadata.AlterColumnFamily_Add_Tag, ""));
                    	cfm.recordColumnDrop(columnName.toString());
                    	break;
                	}
                 	else
                 		throw new InvalidRequestException(String.format("Column %s was not found in table %s", columnName, columnFamily()));
                }

                switch (name.kind)
                {
                    case KEY_ALIAS:
                    case COLUMN_ALIAS:
                        throw new InvalidRequestException(String.format("Cannot drop PRIMARY KEY part %s", columnName));
                    case COLUMN_METADATA:
                        for (ColumnDefinition columnDef : cfm.getColumn_metadata().values())
                        {
                            if (columnDef.name.equals(columnName.key)){
                                toDelete = columnDef;
                                // Register deletion in metadata
                                Metadata.mutate(MetadataRegistry.instance.add(keyspace() + "." + columnFamily() + "." + columnName.toString(),
                            		   Metadata.AlterColumnFamily_Drop_Tag, ""));
                                removeColDef = true;
                            }
                        }
                        assert toDelete != null;
                        if(removeColDef){
                        	cfm.removeColumnDefinition(toDelete);
                        }
                }
                
                logValue = "column_name=" + columnName.toString();
                MetadataLog.announce(keyspace() + "." + columnFamily(), Metadata.AlterColumnFamily_Drop_Tag, clientState, logValue);
                
                break;
            case OPTS:
                if (cfProps == null)
                    throw new InvalidRequestException(String.format("ALTER COLUMNFAMILY WITH invoked, but no parameters found"));

                cfProps.validate();
                cfProps.applyToCFMetadata(cfm);
                
                //TODO: fix the format and add old value
                logValue = "New: " + cfProps.toString();
                MetadataLog.announce(keyspace() + "." + columnFamily(), Metadata.AlterColumnFamily_Prob_Tag, clientState, logValue);
	            
                break;
            case RENAME:
         
                if (cfm.getKeyAliases().size() < cfDef.keys.size() && !renamesAllAliases(cfDef, renames.keySet(), CFDefinition.Name.Kind.KEY_ALIAS, cfDef.keys.size()))
                    throw new InvalidRequestException("When upgrading from Thrift, all the columns of the (composite) partition key must be renamed together.");
                if (cfm.getColumnAliases().size() < cfDef.columns.size() && !renamesAllAliases(cfDef, renames.keySet(), CFDefinition.Name.Kind.COLUMN_ALIAS, cfDef.columns.size()))
                    throw new InvalidRequestException("When upgrading from Thrift, all the columns of the (composite) clustering key must be renamed together.");

                for (Map.Entry<ColumnIdentifier, ColumnIdentifier> entry : renames.entrySet())
                {
                    CFDefinition.Name from = cfDef.get(entry.getKey());
                    ColumnIdentifier to = entry.getValue();
                    if (from == null)
                        throw new InvalidRequestException(String.format("Column %s was not found in table %s", entry.getKey(), columnFamily()));

                    CFDefinition.Name existCF = cfDef.get(to);
                    if (existCF != null)
                        throw new InvalidRequestException(String.format("Cannot rename column %s in table %s to %s; another column of that name already exist", from, columnFamily(), to));

                    logValue += "from: " + from.name.toString() + ", to: " + to.toString() + "; " ;
                    		
                    switch (from.kind)
                    {
                        case KEY_ALIAS:
                            cfm.keyAliases(rename(from.position, to, cfm.getKeyAliases()));
                            break;
                        case COLUMN_ALIAS:
                            cfm.columnAliases(rename(from.position, to, cfm.getColumnAliases()));
                            break;
                        case VALUE_ALIAS:
                            cfm.valueAlias(to.key);
                            break;
                        case COLUMN_METADATA:
                            throw new InvalidRequestException(String.format("Cannot rename non PRIMARY KEY part %s", from));
                    }
                }
                
                MetadataLog.announce(keyspace() + "." + columnFamily(), Metadata.AlterColumnFamily_Rename_Tag, clientState, logValue);
	            
                break;
        }

        MigrationManager.announceColumnFamilyUpdate(cfm);
    }

    private static boolean renamesAllAliases(CFDefinition cfDef, Set<ColumnIdentifier> names, CFDefinition.Name.Kind kind, int expected)
    {
        int renamed = Sets.filter(names, isA(cfDef, kind)).size();
        return renamed == 0 || renamed == expected;
    }

    private static Predicate<ColumnIdentifier> isA(final CFDefinition cfDef, final CFDefinition.Name.Kind kind)
    {
        return new Predicate<ColumnIdentifier>()
        {
            public boolean apply(ColumnIdentifier input)
            {
                CFDefinition.Name name = cfDef.get(input);
                return name != null && name.kind == kind;
            }
        };
    }

    private static List<ByteBuffer> rename(int pos, ColumnIdentifier newName, List<ByteBuffer> aliases)
    {
        if (pos < aliases.size())
        {
            List<ByteBuffer> newList = new ArrayList<ByteBuffer>(aliases);
            newList.set(pos, newName.key);
            return newList;
        }
        else
        {
            // We insert nulls temporarly, but have checked that all the aliases are renamed
            List<ByteBuffer> newList = new ArrayList<ByteBuffer>(pos + 1);
            for (int i = 0; i < pos; ++i)
                newList.add(i < aliases.size() ? aliases.get(i) : null);
            newList.add(newName.key);
            return newList;
        }
    }

    public String toString()
    {
        return String.format("AlterTableStatement(name=%s, type=%s, column=%s, validator=%s)",
                             cfName,
                             oType,
                             columnName,
                             validator);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.UPDATED;
    }
}
