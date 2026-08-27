-- =====================================================================
-- Nimbo Batch Processor - Seed Protocol Dimension Data (V2)
-- =====================================================================

INSERT INTO protocol_dim (id, name, description) VALUES
    (1, 'ICMP', 'Internet Control Message Protocol'),
    (2, 'IGMP', 'Internet Group Management Protocol'),
    (6, 'TCP', 'Transmission Control Protocol'),
    (17, 'UDP', 'User Datagram Protocol'),
    (50, 'ESP', 'Encapsulating Security Payload'),
    (51, 'AH', 'Authentication Header'),
    (89, 'OSPF', 'Open Shortest Path First'),
    (132, 'SCTP', 'Stream Control Transmission Protocol')
    ON CONFLICT (id) DO NOTHING;

