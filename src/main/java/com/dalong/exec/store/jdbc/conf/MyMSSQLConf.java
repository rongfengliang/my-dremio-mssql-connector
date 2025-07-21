package com.dalong.exec.store.jdbc.conf;

import com.dalong.exec.store.jdbc.dialect.MyMSSQLDialect;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.conf.AbstractArpConf;
import com.dremio.exec.store.jdbc.conf.BaseMSSQLConf;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

@SourceType(
        value = "MYMSSQL",
        uiConfig = "my-mssql-layout.json",
        label = "Microsoft SQL Server",
        externalQuerySupported = true,
        previewEngineRequired = true
)
public class MyMSSQLConf extends BaseMSSQLConf {
    private static final String ARP_FILENAME = "arp/implementation/my-mssql-arp.yaml";
    private static final MyMSSQLDialect MS_ARP_DIALECT = (MyMSSQLDialect)AbstractArpConf.loadArpFile(ARP_FILENAME, MyMSSQLDialect::new);
    @Tag(8)
    @DisplayMetadata(
            label = "Database (optional)"
    )
    public String database;
    @Tag(9)
    @DisplayMetadata(
            label = "Show only the initial database used for connecting"
    )
    public boolean showOnlyConnectionDatabase = false;

    public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
        return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withDatabase(this.database).withShowOnlyConnDatabase(this.showOnlyConnectionDatabase).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
    }

    protected String getDatabase() {
        return this.database;
    }

    public MyMSSQLDialect getDialect() {
        return MS_ARP_DIALECT;
    }

    @VisibleForTesting
    public static MyMSSQLDialect getDialectSingleton() {
        return MS_ARP_DIALECT;
    }
}
