/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.iceberg.statistics.StatisticsFileCache;
import com.facebook.presto.iceberg.util.IcebergPrestoModelConverters;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterStatsCalculatorService;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getCompressionCodec;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.iceberg.IcebergUtil.VIEW_OWNER;
import static com.facebook.presto.iceberg.IcebergUtil.createIcebergViewProperties;
import static com.facebook.presto.iceberg.IcebergUtil.getColumns;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergTable;
import static com.facebook.presto.iceberg.IcebergUtil.getNativeIcebergView;
import static com.facebook.presto.iceberg.IcebergUtil.populateTableProperties;
import static com.facebook.presto.iceberg.PartitionFields.parsePartitionFields;
import static com.facebook.presto.iceberg.PartitionSpecConverter.toPrestoPartitionSpec;
import static com.facebook.presto.iceberg.SchemaConverter.toPrestoSchema;
import static com.facebook.presto.iceberg.SortFieldUtils.parseSortFields;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergNamespace;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toPrestoNamespace;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toPrestoSchemaName;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toPrestoSchemaTableName;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.iceberg.NullOrder.NULLS_FIRST;
import static org.apache.iceberg.NullOrder.NULLS_LAST;

public class IcebergNativeMetadata
        extends IcebergAbstractMetadata
{
    private static final String VIEW_DIALECT = "presto";

    private final Optional<String> warehouseDataDir;
    private final IcebergNativeCatalogFactory catalogFactory;
    private final CatalogType catalogType;
    private final ConcurrentMap<SchemaTableName, View> icebergViews = new ConcurrentHashMap<>();

    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaces;
    private final Cache<TableIdentifier, TableIdentifier> remoteTables;
    private final Cache<TableIdentifier, TableIdentifier> remoteViews;

    public IcebergNativeMetadata(
            IcebergNativeCatalogFactory catalogFactory,
            TypeManager typeManager,
            StandardFunctionResolution functionResolution,
            RowExpressionService rowExpressionService,
            JsonCodec<CommitTaskData> commitTaskCodec,
            CatalogType catalogType,
            NodeVersion nodeVersion,
            FilterStatsCalculatorService filterStatsCalculatorService,
            StatisticsFileCache statisticsFileCache,
            IcebergTableProperties tableProperties)
    {
        super(typeManager, functionResolution, rowExpressionService, commitTaskCodec, nodeVersion, filterStatsCalculatorService, statisticsFileCache, tableProperties);
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogType = requireNonNull(catalogType, "catalogType is null");
        this.warehouseDataDir = Optional.ofNullable(catalogFactory.getCatalogWarehouseDataDir());
        this.caseInsensitiveNameMatching = catalogFactory.isCaseInsensitiveNameMatching();
        this.remoteNamespaces = catalogFactory.getRemoteNamespacesCache();
        this.remoteTables = catalogFactory.getRemoteTablesCache();
        this.remoteViews = catalogFactory.getRemoteViewsCache();
    }

    @Override
    protected Table getRawIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return getNativeIcebergTable(catalogFactory, session, toRemoteTable(session, schemaTableName));
    }

    @Override
    protected View getIcebergView(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return icebergViews.computeIfAbsent(
                schemaTableName,
                ignored -> getNativeIcebergView(catalogFactory, session, toRemoteView(session, schemaTableName)));
    }

    @Override
    protected boolean tableExists(ConnectorSession session, SchemaTableName schemaTableName)
    {
        IcebergTableName name = IcebergTableName.from(schemaTableName.getTableName());
        try {
            getIcebergTable(session, new SchemaTableName(schemaTableName.getSchemaName(), name.getTableName()));
        }
        catch (NoSuchTableException e) {
            // return null to throw
            return false;
        }
        return true;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        SupportsNamespaces supportsNamespaces = catalogFactory.getNamespaces(session);
        if (catalogFactory.isNestedNamespaceEnabled()) {
            return listNestedNamespaces(supportsNamespaces, Namespace.empty());
        }

        return supportsNamespaces.listNamespaces()
                .stream()
                .map(IcebergPrestoModelConverters::toPrestoSchemaName)
                .collect(toList());
    }

    private List<String> listNestedNamespaces(SupportsNamespaces supportsNamespaces, Namespace parentNamespace)
    {
        return supportsNamespaces.listNamespaces(parentNamespace)
                .stream()
                .flatMap(childNamespace -> Stream.concat(
                        Stream.of(toPrestoSchemaName(childNamespace)),
                        listNestedNamespaces(supportsNamespaces, childNamespace).stream()))
                .collect(toList());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent() && INFORMATION_SCHEMA.equals(schemaName.get())) {
            return listSchemaNames(session).stream()
                    .map(schema -> new SchemaTableName(INFORMATION_SCHEMA, schema))
                    .collect(toList());
        }

        try {
            return catalogFactory.getCatalog(session).listTables(toRemoteNamespace(session, schemaName.orElse(null)))
                    .stream()
                    .map(tableIdentifier -> toPrestoSchemaTableName(tableIdentifier, catalogFactory.isNestedNamespaceEnabled()))
                    .collect(toImmutableList());
        }
        catch (NoSuchNamespaceException e) {
            return ImmutableList.of();
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        catalogFactory.getNamespaces(session).createNamespace(toIcebergNamespace(schemaName, catalogFactory.isNestedNamespaceEnabled()),
                properties.entrySet().stream()
                        .collect(toMap(Map.Entry::getKey, e -> e.getValue().toString())));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        Namespace namespace = toRemoteNamespace(session, schemaName);

        try {
            catalogFactory.getNamespaces(session).dropNamespace(namespace);
        }
        catch (NamespaceNotEmptyException e) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }

        if (caseInsensitiveNameMatching) {
            remoteNamespaces.invalidate(namespace);
        }
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        throw new PrestoException(NOT_SUPPORTED, format("Iceberg %s catalog does not support rename namespace", catalogType.name()));
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support creating views");
        }
        Schema schema = toIcebergSchema(viewMetadata.getColumns());
        ViewBuilder viewBuilder = ((ViewCatalog) catalog).buildView(toRemoteView(session, viewMetadata.getTable()))
                .withSchema(schema)
                .withDefaultNamespace(toRemoteNamespace(session, viewMetadata.getTable().getSchemaName()))
                .withQuery(VIEW_DIALECT, viewData)
                .withProperties(createIcebergViewProperties(session, nodeVersion.toString()));
        if (replace) {
            viewBuilder.createOrReplace();
        }
        else {
            viewBuilder.create();
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        Catalog catalog = catalogFactory.getCatalog(session);
        if (catalog instanceof ViewCatalog) {
            for (String schema : listSchemas(session, schemaName.orElse(null))) {
                try {
                    for (TableIdentifier tableIdentifier : ((ViewCatalog) catalog).listViews(
                            toRemoteNamespace(session, schema))) {
                        tableNames.add(new SchemaTableName(schema, tableIdentifier.name()));
                    }
                }
                catch (NoSuchNamespaceException e) {
                    // ignore
                }
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return listSchemaNames(session);
        }
        return ImmutableList.of(schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views = ImmutableMap.builder();
        Catalog catalog = catalogFactory.getCatalog(session);
        if (catalog instanceof ViewCatalog) {
            List<SchemaTableName> tableNames;
            if (prefix.getTableName() != null) {
                tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
            }
            else {
                tableNames = listViews(session, Optional.of(prefix.getSchemaName()));
            }

            for (SchemaTableName schemaTableName : tableNames) {
                try {
                    TableIdentifier viewIdentifier = toRemoteView(session, schemaTableName);
                    if (((ViewCatalog) catalog).viewExists(viewIdentifier)) {
                        View view = ((ViewCatalog) catalog).loadView(viewIdentifier);
                        verifyAndPopulateViews(view, schemaTableName, view.sqlFor(VIEW_DIALECT).sql(), views);
                    }
                }
                catch (IllegalArgumentException e) {
                    // Ignore illegal view names
                }
            }
        }
        return views.build();
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        Catalog catalog = catalogFactory.getCatalog(session);
        if (!(catalog instanceof ViewCatalog)) {
            throw new PrestoException(NOT_SUPPORTED, "This connector does not support dropping views");
        }

        TableIdentifier viewIdentifier = toRemoteView(session, viewName);
        ((ViewCatalog) catalog).dropView(viewIdentifier);

        if (caseInsensitiveNameMatching) {
            remoteTables.invalidate(viewIdentifier);
        }
    }

    private void verifyAndPopulateViews(View view, SchemaTableName schemaTableName, String viewData, ImmutableMap.Builder<SchemaTableName, ConnectorViewDefinition> views)
    {
        views.put(schemaTableName, new ConnectorViewDefinition(
                schemaTableName,
                Optional.ofNullable(view.properties().get(VIEW_OWNER)),
                viewData));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = toIcebergSchema(tableMetadata.getColumns());

        PartitionSpec partitionSpec = parsePartitionFields(schema, tableProperties.getPartitioning(tableMetadata.getProperties()));
        FileFormat fileFormat = tableProperties.getFileFormat(session, tableMetadata.getProperties());

        try {
            TableIdentifier tableIdentifier = toRemoteTable(session, schemaTableName);
            String targetPath = tableProperties.getTableLocation(tableMetadata.getProperties());
            if (!isNullOrEmpty(targetPath)) {
                transaction = catalogFactory.getCatalog(session).newCreateTableTransaction(
                        tableIdentifier,
                        schema,
                        partitionSpec,
                        targetPath,
                        populateTableProperties(this, tableMetadata, tableProperties, fileFormat, session));
            }
            else {
                transaction = catalogFactory.getCatalog(session).newCreateTableTransaction(
                        tableIdentifier,
                        schema,
                        partitionSpec,
                        populateTableProperties(this, tableMetadata, tableProperties, fileFormat, session));
            }
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(schemaTableName);
        }

        Table icebergTable = transaction.table();
        ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
        SortOrder sortOrder = parseSortFields(schema, tableProperties.getSortOrder(tableMetadata.getProperties()));
        List<SortField> sortFields = getSupportedSortFields(icebergTable.schema(), sortOrder);
        for (SortField sortField : sortFields) {
            if (sortField.getSortOrder().isAscending()) {
                replaceSortOrder.asc(schema.findColumnName(sortField.getSourceColumnId()), sortField.getSortOrder().isNullsFirst() ? NULLS_FIRST : NULLS_LAST);
            }
            else {
                replaceSortOrder.desc(schema.findColumnName(sortField.getSourceColumnId()), sortField.getSortOrder().isNullsFirst() ? NULLS_FIRST : NULLS_LAST);
            }
        }

        try {
            replaceSortOrder.commit();
        }
        catch (RuntimeException e) {
            throw new PrestoException(ICEBERG_COMMIT_ERROR, "Failed to set the sorted_by property", e);
        }

        return new IcebergOutputTableHandle(
                schemaName,
                new IcebergTableName(tableName, DATA, Optional.empty(), Optional.empty()),
                toPrestoSchema(icebergTable.schema(), typeManager),
                toPrestoPartitionSpec(icebergTable.spec(), typeManager),
                getColumns(icebergTable.schema(), icebergTable.spec(), typeManager),
                icebergTable.location(),
                fileFormat,
                getCompressionCodec(session),
                icebergTable.properties(),
                sortFields);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be dropped");
        TableIdentifier tableIdentifier = toRemoteTable(session, icebergTableHandle.getSchemaTableName());
        catalogFactory.getCatalog(session).dropTable(tableIdentifier);

        if (caseInsensitiveNameMatching) {
            remoteTables.invalidate(tableIdentifier);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        verify(icebergTableHandle.getIcebergTableName().getTableType() == DATA, "only the data table can be renamed");
        TableIdentifier from = toRemoteTable(session, icebergTableHandle.getSchemaTableName());
        TableIdentifier to = toRemoteTable(session, newTable);
        catalogFactory.getCatalog(session).renameTable(from, to);

        if (caseInsensitiveNameMatching) {
            remoteTables.invalidate(from);
        }
    }

    @Override
    public void registerTable(ConnectorSession clientSession, SchemaTableName schemaTableName, Path metadataLocation)
    {
        TableIdentifier tableIdentifier = TableIdentifier.of(toRemoteNamespace(clientSession, schemaTableName.getSchemaName()), schemaTableName.getTableName());
        catalogFactory.getCatalog(clientSession).registerTable(tableIdentifier, metadataLocation.toString());
    }

    @Override
    public void unregisterTable(ConnectorSession clientSession, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = toRemoteTable(clientSession, schemaTableName);
        catalogFactory.getCatalog(clientSession).dropTable(tableIdentifier, false);

        if (caseInsensitiveNameMatching) {
            remoteTables.invalidate(tableIdentifier);
        }
    }

    protected Optional<String> getDataLocationBasedOnWarehouseDataDir(SchemaTableName schemaTableName)
    {
        if (!catalogType.equals(HADOOP)) {
            return Optional.empty();
        }
        return warehouseDataDir.map(base -> base + schemaTableName.getSchemaName() + "/" + schemaTableName.getTableName());
    }

    private Namespace toRemoteNamespace(ConnectorSession session, String schemaName)
    {
        Namespace namespace = toIcebergNamespace(schemaName, catalogFactory.isNestedNamespaceEnabled());
        if (caseInsensitiveNameMatching) {
            try {
                return remoteNamespaces.get(namespace, () -> findRemoteNamespace(session, namespace));
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Unexpected checked exception from cache load", e);
            }
        }

        return namespace;
    }

    private Namespace findRemoteNamespace(ConnectorSession session, Namespace namespace)
    {
        SupportsNamespaces supportsNamespaces = catalogFactory.getNamespaces(session);
        List<Namespace> matchingRemoteNamespaces = listRemoteNamespaces(supportsNamespaces, Namespace.empty()).stream()
                .filter(ns -> toPrestoNamespace(ns).equals(namespace))
                .collect(toImmutableList());

        return matchingRemoteNamespaces.isEmpty() ? namespace : matchingRemoteNamespaces.get(0);
    }

    private List<Namespace> listRemoteNamespaces(SupportsNamespaces supportsNamespaces, Namespace parentNamespace)
    {
        List<Namespace> childNamespaces = supportsNamespaces.listNamespaces(parentNamespace);
        return childNamespaces.stream()
                .flatMap(childNamespace -> Stream.concat(
                        Stream.of(childNamespace),
                        listRemoteNamespaces(supportsNamespaces, childNamespace).stream()))
                .collect(toList());
    }

    private TableIdentifier toRemoteTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(schemaTableName, catalogFactory.isNestedNamespaceEnabled());
        if (caseInsensitiveNameMatching) {
            try {
                return remoteTables.get(tableIdentifier, () -> findRemoteTable(session, tableIdentifier));
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Unexpected checked exception from cache load", e);
            }
        }

        return tableIdentifier;
    }

    private TableIdentifier findRemoteTable(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace().toString());
        Catalog icebergRestCatalog = catalogFactory.getCatalog(session);
        List<TableIdentifier> matchingTableIdentifiers = icebergRestCatalog.listTables(remoteNamespace).stream()
                .filter(ti -> ti.name().equalsIgnoreCase(tableIdentifier.name()))
                .collect(toImmutableList());

        return matchingTableIdentifiers.isEmpty() ? TableIdentifier.of(remoteNamespace, tableIdentifier.name()) : matchingTableIdentifiers.get(0);
    }

    private TableIdentifier toRemoteView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        TableIdentifier tableIdentifier = toIcebergTableIdentifier(schemaViewName, catalogFactory.isNestedNamespaceEnabled());
        if (caseInsensitiveNameMatching) {
            try {
                return remoteViews.get(tableIdentifier, () -> findRemoteView(session, tableIdentifier));
            }
            catch (ExecutionException e) {
                throw new RuntimeException("Unexpected checked exception from cache load", e);
            }
        }

        return tableIdentifier;
    }

    private TableIdentifier findRemoteView(ConnectorSession session, TableIdentifier tableIdentifier)
    {
        Namespace remoteNamespace = toRemoteNamespace(session, tableIdentifier.namespace().toString());
        ViewCatalog viewRestCatalog = (ViewCatalog) catalogFactory.getCatalog(session);
        List<TableIdentifier> matchingTableIdentifiers = viewRestCatalog.listViews(remoteNamespace).stream()
                .filter(ti -> ti.name().equalsIgnoreCase(tableIdentifier.name()))
                .collect(toImmutableList());

        return matchingTableIdentifiers.isEmpty() ? TableIdentifier.of(remoteNamespace, tableIdentifier.name()) : matchingTableIdentifiers.get(0);
    }
}
