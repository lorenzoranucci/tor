CREATE TABLE IF NOT EXISTS my_schema.my_outbox_table
(
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    payload        LONGBLOB     NOT NULL
);
