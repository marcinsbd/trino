package io.trino.plugin.iceberg.catalog;

import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class AbstractTrinoCatalogWrapper
        extends AbstractTrinoCatalog
{
    private final AbstractTrinoCatalog delegate;

    protected AbstractTrinoCatalogWrapper(AbstractTrinoCatalog delegate)
    {
        super(delegate.catalogName, delegate.typeManager, delegate.tableOperationsProvider, delegate.useUniqueTableLocation);
        this.delegate = delegate;
    }

    @Override
    protected Optional<ConnectorMaterializedViewDefinition> doGetMaterializedView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        return delegate.doGetMaterializedView(session, schemaViewName);
    }

    @Override
    public boolean namespaceExists(ConnectorSession session, String namespace)
    {
        return delegate.namespaceExists(session, namespace);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return delegate.listNamespaces(session);
    }

    @Override
    public void dropNamespace(ConnectorSession session, String namespace)
    {
        delegate.dropNamespace(session, namespace);
    }

    @Override
    public Map<String, Object> loadNamespaceMetadata(ConnectorSession session, String namespace)
    {
        return delegate.loadNamespaceMetadata(session, namespace);
    }

    @Override
    public Optional<TrinoPrincipal> getNamespacePrincipal(ConnectorSession session, String namespace)
    {
        return delegate.getNamespacePrincipal(session, namespace);
    }

    @Override
    public void createNamespace(ConnectorSession session, String namespace, Map<String, Object> properties, TrinoPrincipal owner)
    {
        delegate.createNamespace(session, namespace, properties, owner);
    }

    @Override
    public void setNamespacePrincipal(ConnectorSession session, String namespace, TrinoPrincipal principal)
    {
        delegate.setNamespacePrincipal(session, namespace, principal);
    }

    @Override
    public void renameNamespace(ConnectorSession session, String source, String target)
    {
        delegate.renameNamespace(session, source, target);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        return delegate.listTables(session, namespace);
    }

    @Override
    public Transaction newCreateTableTransaction(ConnectorSession session, SchemaTableName schemaTableName, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder, String location, Map<String, String> properties)
    {
        return delegate.newCreateTableTransaction(session, schemaTableName, schema, partitionSpec, sortOrder, location, properties);
    }

    @Override
    public void registerTable(ConnectorSession session, SchemaTableName tableName, String tableLocation, String metadataLocation)
    {
        delegate.registerTable(session, tableName, tableLocation, metadataLocation);
    }

    @Override
    public void unregisterTable(ConnectorSession session, SchemaTableName tableName)
    {
        delegate.unregisterTable(session,tableName);
    }

    @Override
    public void dropTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        delegate.dropTable(session, schemaTableName);
    }

    @Override
    public void dropCorruptedTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        delegate.dropCorruptedTable(session, schemaTableName);
    }

    @Override
    public void renameTable(ConnectorSession session, SchemaTableName from, SchemaTableName to)
    {
        delegate.renameTable(session, from, to);
    }

    @Override
    public Table loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return delegate.loadTable(session, schemaTableName);
    }

    @Override
    public void updateViewComment(ConnectorSession session, SchemaTableName schemaViewName, Optional<String> comment)
    {
        delegate.updateViewComment(session, schemaViewName, comment);
    }

    @Override
    public void updateViewColumnComment(ConnectorSession session, SchemaTableName schemaViewName, String columnName, Optional<String> comment)
    {
        delegate.updateViewColumnComment(session, schemaViewName, columnName, comment);
    }

    @Override
    public String defaultTableLocation(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return delegate.defaultTableLocation(session, schemaTableName);
    }

    @Override
    public void setTablePrincipal(ConnectorSession session, SchemaTableName schemaTableName, TrinoPrincipal principal)
    {
        delegate.setTablePrincipal(session, schemaTableName, principal);
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName schemaViewName, ConnectorViewDefinition definition, boolean replace)
    {
        delegate.createView(session, schemaViewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        delegate.renameView(session, source, target);
    }

    @Override
    public void setViewPrincipal(ConnectorSession session, SchemaTableName schemaViewName, TrinoPrincipal principal)
    {
        delegate.setViewPrincipal(session, schemaViewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName schemaViewName)
    {
        delegate.dropView(session, schemaViewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> namespace)
    {
        return delegate.listViews(session, namespace);
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return delegate.getView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> namespace)
    {
        return delegate.listMaterializedViews(session, namespace);
    }

    @Override
    public void createMaterializedView(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition, boolean replace, boolean ignoreExisting)
    {
        delegate.createMaterializedView(session, viewName, definition, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        delegate.dropMaterializedView(session, viewName);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        delegate.renameMaterializedView(session,source,target);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        return delegate.redirectTable(session, tableName);
    }
}
