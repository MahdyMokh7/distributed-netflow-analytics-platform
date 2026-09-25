CREATE TABLE IF NOT EXISTS nimbo.netflow
(

    ipv4_src_addr VARCHAR(15) NOT NULL,
    ipv4_dst_addr VARCHAR(15) NOT NULL,
    kafka_timestamp BIGINT NOT NULL,


    kafka_topic VARCHAR(255) NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,

    l4_src_port INT NOT NULL,
    l4_dst_port INT NOT NULL,

    protocol INT NOT NULL,
    l7_proto DOUBLE NOT NULL,

    in_bytes BIGINT NOT NULL,
    out_bytes BIGINT NOT NULL,
    in_pkts BIGINT NOT NULL,
    out_pkts BIGINT NOT NULL,

    flow_duration_milliseconds BIGINT NOT NULL,

    tcp_flags INT NOT NULL,
    client_tcp_flags INT NOT NULL,
    server_tcp_flags INT NOT NULL,

    duration_in BIGINT NOT NULL,
    duration_out BIGINT NOT NULL,

    min_ttl INT NOT NULL,
    max_ttl INT NOT NULL,

    longest_flow_pkt INT NOT NULL,
    shortest_flow_pkt INT NOT NULL,

    min_ip_pkt_len INT NOT NULL,
    max_ip_pkt_len INT NOT NULL,

    src_to_dst_second_bytes BIGINT NOT NULL,
    dst_to_src_second_bytes BIGINT NOT NULL,

    retransmitted_in_bytes BIGINT NOT NULL,
    retransmitted_in_pkts BIGINT NOT NULL,
    retransmitted_out_bytes BIGINT NOT NULL,
    retransmitted_out_pkts BIGINT NOT NULL,

    src_to_dst_avg_throughput BIGINT NOT NULL,
    dst_to_src_avg_throughput BIGINT NOT NULL,

    num_pkts_up_to_128_bytes BIGINT NOT NULL,
    num_pkts_128_to_256_bytes BIGINT NOT NULL,
    num_pkts_256_to_512_bytes BIGINT NOT NULL,
    num_pkts_512_to_1024_bytes BIGINT NOT NULL,
    num_pkts_1024_to_1514_bytes BIGINT NOT NULL,

    tcp_win_max_in INT NOT NULL,
    tcp_win_max_out INT NOT NULL,

    icmp_type INT NOT NULL,
    icmp_ipv4_type INT NOT NULL,

    dns_query_id INT NOT NULL,
    dns_query_type INT NOT NULL,
    dns_ttl_answer BIGINT NOT NULL,

    ftp_command_ret_code INT NOT NULL
    )
    ENGINE = OLAP

    DUPLICATE KEY(
                     ipv4_src_addr,
                     ipv4_dst_addr
                 )

    PARTITION BY date_trunc(
                               'day',
                               from_unixtime_ms(kafka_timestamp)
    )

    DISTRIBUTED BY HASH(ipv4_src_addr, ipv4_dst_addr)
    BUCKETS 8

