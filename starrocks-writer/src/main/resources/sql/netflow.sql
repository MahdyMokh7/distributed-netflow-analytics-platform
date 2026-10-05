CREATE TABLE IF NOT EXISTS nimbo.netflow
(
    ipv4_src_addr VARCHAR(15) COMMENT "IPv4 source address.",
    ipv4_dst_addr VARCHAR(15) COMMENT "IPv4 destination address.",
    PROCESS_TIME BIGINT NOT NULL COMMENT "Kafka record timestamp in milliseconds since the Unix epoch.",

    kafka_topic VARCHAR(255) NOT NULL COMMENT "Kafka topic from which the record was consumed.",
    kafka_partition INT NOT NULL COMMENT "Kafka partition from which the record was consumed.",
    kafka_offset BIGINT NOT NULL COMMENT "Kafka offset of the consumed record.",

    l4_src_port INT COMMENT "Layer 4 source port number.",
    l4_dst_port INT COMMENT "Layer 4 destination port number.",

    protocol INT COMMENT "IP protocol identifier.",
    l7_proto DOUBLE COMMENT "Layer 7 protocol numeric identifier.",

    in_bytes BIGINT COMMENT "Incoming byte count.",
    out_bytes BIGINT COMMENT "Outgoing byte count.",
    in_pkts BIGINT COMMENT "Incoming packet count.",
    out_pkts BIGINT COMMENT "Outgoing packet count.",

    flow_duration_milliseconds BIGINT COMMENT "Flow duration in milliseconds.",

    tcp_flags INT COMMENT "Cumulative TCP flags.",
    client_tcp_flags INT COMMENT "Cumulative client TCP flags.",
    server_tcp_flags INT COMMENT "Cumulative server TCP flags.",

    duration_in BIGINT COMMENT "Client-to-server stream duration in milliseconds.",
    duration_out BIGINT COMMENT "Server-to-client stream duration in milliseconds.",

    min_ttl INT COMMENT "Minimum observed flow TTL.",
    max_ttl INT COMMENT "Maximum observed flow TTL.",

    longest_flow_pkt INT COMMENT "Longest flow packet size in bytes.",
    shortest_flow_pkt INT COMMENT "Shortest flow packet size in bytes.",

    min_ip_pkt_len INT COMMENT "Smallest observed IP packet length.",
    max_ip_pkt_len INT COMMENT "Largest observed IP packet length.",

    src_to_dst_second_bytes BIGINT COMMENT "Source-to-destination bytes per second.",
    dst_to_src_second_bytes BIGINT COMMENT "Destination-to-source bytes per second.",

    retransmitted_in_bytes BIGINT COMMENT "Retransmitted TCP bytes from source to destination.",
    retransmitted_in_pkts BIGINT COMMENT "Retransmitted TCP packets from source to destination.",
    retransmitted_out_bytes BIGINT COMMENT "Retransmitted TCP bytes from destination to source.",
    retransmitted_out_pkts BIGINT COMMENT "Retransmitted TCP packets from destination to source.",

    src_to_dst_avg_throughput BIGINT COMMENT "Source-to-destination average throughput in bits per second.",
    dst_to_src_avg_throughput BIGINT COMMENT "Destination-to-source average throughput in bits per second.",

    num_pkts_up_to_128_bytes BIGINT COMMENT "Packets with IP size up to and including 128 bytes.",
    num_pkts_128_to_256_bytes BIGINT COMMENT "Packets with IP size greater than 128 and up to 256 bytes.",
    num_pkts_256_to_512_bytes BIGINT COMMENT "Packets with IP size greater than 256 and up to 512 bytes.",
    num_pkts_512_to_1024_bytes BIGINT COMMENT "Packets with IP size greater than 512 and up to 1024 bytes.",
    num_pkts_1024_to_1514_bytes BIGINT COMMENT "Packets with IP size greater than 1024 and up to 1514 bytes.",

    tcp_win_max_in INT COMMENT "Maximum TCP window from source to destination.",
    tcp_win_max_out INT COMMENT "Maximum TCP window from destination to source.",

    icmp_type INT COMMENT "Combined ICMP type and code: type multiplied by 256 plus code.",
    icmp_ipv4_type INT COMMENT "ICMPv4 type.",

    dns_query_id INT COMMENT "DNS query transaction identifier.",
    dns_query_type INT COMMENT "DNS query type, such as 1 for A or 2 for NS.",
    dns_ttl_answer BIGINT COMMENT "TTL of the first DNS A record answer, if present.",

    ftp_command_ret_code INT COMMENT "FTP client command return code."
)
ENGINE = OLAP

DUPLICATE KEY(
    ipv4_src_addr,
    ipv4_dst_addr
)

PARTITION BY date_trunc(
    'day',
    from_unixtime_ms(PROCESS_TIME)
)

DISTRIBUTED BY HASH(ipv4_src_addr, ipv4_dst_addr)
BUCKETS 8
PROPERTIES (
    "partition_live_number" = "7",
    "replication_num" = "2",
    "compression" = "ZSTD"
);
