package io.trino.plugin.iceberg.catalog;

import org.apache.iceberg.TableMetadata;

//public class MViewIcebergTableOperations extends AbstractIcebergTableOperations
public class MViewIcebergTableOperations
        extends MViewIcebergTableOperationsWrapper
{

    public MViewIcebergTableOperations(IcebergTableOperations delegate)
    {
        super(delegate);
    }

    @Override
    public void commit(TableMetadata base, TableMetadata tableMetadata)
    {
//        String location = this.writeNewMetadata(tableMetadata, 0);
// ??
    }

}
