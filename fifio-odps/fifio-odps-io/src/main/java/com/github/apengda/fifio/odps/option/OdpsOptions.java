package com.github.apengda.fifio.odps.option;

import com.github.apengda.fifio.jdbc.frame.DbInfo;
import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;
import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class OdpsOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final String project;
    protected final String table;
    protected final String accessId;
    protected final String accessKey;
    @Nullable
    protected final String partition;
    private final String odpsUrl;


    public OdpsOptions(String odpsUrl, String project, String table, String accessId,
                       String accessKey, String partition) {
        this.odpsUrl = odpsUrl;
        this.project = project;
        this.table = table;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.partition = partition;
    }

    public static OdpsOptionsBuilder builder() {
        return new OdpsOptionsBuilder();
    }

    public DbInfo toDbInfo() {
        DbInfo info = new DbInfo(odpsUrl, accessId, accessKey);
        info.setDbName(project);
        info.setDbType("odps");
        return info;
    }

    public String getOdpsUrl() {
        return odpsUrl;
    }

    public String getProject() {
        return project;
    }

    public String getTable() {
        return table;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getPartition() {
        return partition;
    }

    public static class OdpsOptionsBuilder {
        protected String project;
        protected String table;
        protected String accessId;
        protected String accessKey;
        @Nullable
        protected String partition;
        private String odpsUrl;

        public OdpsOptionsBuilder setOdpsUrl(String odpsUrl) {
            this.odpsUrl = odpsUrl;
            return this;
        }

        public OdpsOptionsBuilder setProject(String project) {
            this.project = project;
            return this;
        }

        public OdpsOptionsBuilder setTable(String table) {
            this.table = table;
            return this;
        }

        public OdpsOptionsBuilder setAccessId(String accessId) {
            this.accessId = accessId;
            return this;
        }

        public OdpsOptionsBuilder setAccessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public OdpsOptionsBuilder setPartition(@Nullable String partition) {
            this.partition = partition;
            return this;
        }

        public OdpsOptions build() {
            checkNotNull(odpsUrl, "No odpsUrl supplied.");
            checkNotNull(project, "No odps project supplied.");
            checkNotNull(table, "No odps table supplied.");
            checkNotNull(accessId, "No odps accessId supplied.");
            checkNotNull(accessKey, "No odps accessKey supplied.");
            return new OdpsOptions(odpsUrl, project, table, accessId, accessKey, partition);
        }
    }
}
