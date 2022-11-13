CREATE TABLE my_schema.my_outbox_table
(
    id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    payload        LONGBLOB     NOT NULL
);

INSERT INTO my_schema.my_outbox_table (aggregate_type, aggregate_id, payload)
VALUES ('order',
        '40c76224-871d-4a9c-a073-4c1f5dd4a272',
        0xEFBBBF7B226576656E74223A202263726561746564227D),
       ('order',
        '40c76224-871d-4a9c-a073-4c1f5dd4a272',
        '{"event": "deleted"}');
