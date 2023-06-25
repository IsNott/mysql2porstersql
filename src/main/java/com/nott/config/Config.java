package com.nott.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * @author Nott
 * @Date 2023/6/2
 */

@Configuration
public class Config {

    @Bean
    @ConfigurationProperties(prefix = "spring.target-db")
    public DataSource targetDatasource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.source-db")
    @Primary
    public DataSource sourceDataSource() {
        return DataSourceBuilder.create().build();
    }
}
