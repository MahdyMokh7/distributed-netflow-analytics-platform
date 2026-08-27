package com.nimbo.batch.dao;

import com.nimbo.batch.model.BatchStateRecord;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.Set;
import java.util.UUID;

@RegisterBeanMapper(BatchStateRecord.class)
public interface BatchStateDao {

    @SqlQuery("SELECT file_name FROM batch_processor_state")
    Set<String> getProcessedFileNames();

    @SqlQuery("""
        SELECT file_name, file_timestamp, batch_id, processed_at
        FROM batch_processor_state
        WHERE file_name IN (<fileNames>)
        """)
    List<BatchStateRecord> getRecordsByFileNames(@Bind("fileNames") List<String> fileNames);

    @SqlBatch("""
        INSERT INTO batch_processor_state (file_name, file_timestamp, batch_id)
        VALUES (:fileName, :fileTimestamp, :batchId)
        ON CONFLICT (file_name) DO NOTHING
        """)
    int[] markFilesAsProcessed(@BindBean List<BatchStateRecord> records);

    @SqlQuery("SELECT COUNT(*) FROM batch_processor_state")
    long getTotalProcessedFiles();

    @SqlUpdate("DELETE FROM batch_processor_state WHERE processed_at < NOW() - INTERVAL '30 days'")
    int cleanupOldRecords();
}