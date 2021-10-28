package com.github.apengda.fifio.jdbc.frame;


import javax.annotation.Nullable;
import java.io.Serializable;

public class DbInfo implements Serializable {
    protected final String url;
    @Nullable
    protected final String username;
    @Nullable
    protected final String password;
    @Nullable
    protected String dbName;
    @Nullable
    protected String dbType;

    public DbInfo(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDbType() {
        return dbType;
    }

    public DbInfo setDbType(String dbType) {
        this.dbType = dbType;
        return this;
    }

    @Nullable
    public String getDbName() {
        return dbName;
    }

    public DbInfo setDbName(@Nullable String dbName) {
        this.dbName = dbName;
        return this;
    }
}
