CREATE TABLE IF NOT EXISTS nlbwmon
(
    track_id serial not null primary key,
    family integer,
    proto varchar(32),
    port integer,
    mac varchar(32),
    ip character varying(36),
    conns integer,
    rx_bytes bigint,
    rx_pkts bigint,
    tx_bytes bigint,
    tx_pkts bigint,
    layer7 varchar(32),
    month timestamp with time zone
    
);