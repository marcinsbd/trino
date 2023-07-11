package io.trino.plugin.iceberg.catalog;

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

public class MViewIcebergTableOperationsWrapper
        implements IcebergTableOperations
{

    private final IcebergTableOperations delegate;

    public MViewIcebergTableOperationsWrapper(IcebergTableOperations delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public TableMetadata current()
    {
        return delegate.current();
    }

    @Override
    public TableMetadata refresh()
    {
        return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata tableMetadata)
    {
        delegate.commit(base, tableMetadata);
    }

    @Override
    public FileIO io()
    {
        return delegate.io();
    }

    @Override
    public String metadataFileLocation(String s)
    {
        return delegate.metadataFileLocation(s);
    }

    @Override
    public LocationProvider locationProvider()
    {
        return delegate.locationProvider();
    }

    @Override
    public void initializeFromMetadata(TableMetadata tableMetadata)
    {
        this.delegate.initializeFromMetadata(tableMetadata);
    }

    @Override
    public String writeNewMetadata(TableMetadata metadata, int newVersion)
    {
        return this.delegate.writeNewMetadata(metadata, newVersion);
    }

    @Override
    public TableMetadata refreshFromMetadataLocation(String location)
    {
        return this.delegate.refreshFromMetadataLocation(location);
    }
}
