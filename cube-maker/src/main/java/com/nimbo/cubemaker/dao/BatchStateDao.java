package com.nimbo.cubemaker.dao;

import com.nimbo.cubemaker.model.BatchStateRecord;
import java.util.List;
import java.util.Set;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

@RegisterBeanMapper(BatchStateRecord.class)
public interface BatchStateDao {

  @SqlQuery("SELECT file_name FROM cube_maker_state")
  Set<String> getProcessedFileNames();

  @SqlQuery(
      """
      SELECT file_name, file_timestamp, batch_id, processed_at
      FROM cube_maker_state
      WHERE file_name IN (<fileNames>)
      """)
  List<BatchStateRecord> getRecordsByFileNames(@Bind("fileNames") List<String> fileNames);

  @SqlBatch(
      """
      INSERT INTO cube_maker_state (file_name, file_timestamp, batch_id)
      VALUES (:fileName, :fileTimestamp, :batchId)
      ON CONFLICT (file_name) DO NOTHING
      """)
  int[] markFilesAsProcessed(@BindBean List<BatchStateRecord> records);

  @SqlQuery("SELECT COUNT(*) FROM cube_maker_state")
  long getTotalProcessedFiles();

  @SqlUpdate("DELETE FROM cube_maker_state WHERE processed_at < NOW() - INTERVAL '30 days'")
  int cleanupOldRecords();
}
