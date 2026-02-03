create database airflow_x_hevo;


use airflow_x_hevo;


CREATE TABLE no_wait_sync_trigger (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id VARCHAR(64) NOT NULL,      -- identifies one run of the script
    generated_at DATETIME NOT NULL,     -- timestamp for each row
    value_int INT,                      -- example numeric field
    value_text VARCHAR(255),            -- example text field
    payload JSON,                       -- optional structured data
    PRIMARY KEY (id),
    INDEX (batch_id)
);


CREATE TABLE triggerer_example_table (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id VARCHAR(64) NOT NULL,      -- identifies one run of the script
    generated_at DATETIME NOT NULL,     -- timestamp for each row
    value_int INT,                      -- example numeric field
    value_text VARCHAR(255),            -- example text field
    payload JSON,                       -- optional structured data
    PRIMARY KEY (id),
    INDEX (batch_id)
);


CREATE TABLE wait_sync_table_operator (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id VARCHAR(64) NOT NULL,      -- identifies one run of the script
    generated_at DATETIME NOT NULL,     -- timestamp for each row
    value_int INT,                      -- example numeric field
    value_text VARCHAR(255),            -- example text field
    payload JSON,                       -- optional structured data
    PRIMARY KEY (id),
    INDEX (batch_id)
);

CREATE TABLE sensor_wait_example_table (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id VARCHAR(64) NOT NULL,      -- identifies one run of the script
    generated_at DATETIME NOT NULL,     -- timestamp for each row
    value_int INT,                      -- example numeric field
    value_text VARCHAR(255),            -- example text field
    payload JSON,                       -- optional structured data
    PRIMARY KEY (id),
    INDEX (batch_id)
);

CREATE TABLE resync_example_table (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    batch_id VARCHAR(64) NOT NULL,      -- identifies one run of the script
    generated_at DATETIME NOT NULL,     -- timestamp for each row
    value_int INT,                      -- example numeric field
    value_text VARCHAR(255),            -- example text field
    payload JSON,                       -- optional structured data
    PRIMARY KEY (id),
    INDEX (batch_id)
);

