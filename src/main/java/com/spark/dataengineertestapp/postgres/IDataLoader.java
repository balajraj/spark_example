package com.spark.dataengineertestapp.postgres;


public interface IDataLoader
{
    /**
     * Load data with any format to any Database's type.
     *
     * @param connectionUrl: connection url to database
     * @param tableName      : table name in db.
     * @param filePath       : file to upload
     */
    void toDb( String connectionUrl, String tableName, String filePath);
}
