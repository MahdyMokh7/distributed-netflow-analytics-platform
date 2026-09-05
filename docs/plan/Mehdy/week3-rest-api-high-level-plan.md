# 🚀 REST-API Module - High-Level Development Plan

Based on your comprehensive documentation, I'll provide a production-grade high-level plan following software engineering best practices.

---

## 📋 Executive Summary

**Module:** `rest-api`  
**Type:** Read-Only Query Module  
**Primary Tables:** traffic_aggregates, connection_aggregates, protocol_usage_aggregates, ip_bandwidth_aggregates  
**Key Pattern:** CQRS (Read Side), Repository Pattern, DTO Projection

---

## 🎯 1. Architecture Principles

### 1.1 Core Design Decisions

| Principle | Implementation | Rationale |
|-----------|---------------|-----------|
| **Single Responsibility** | Separate controllers, services, repositories, mappers | Maintainability, testability |
| **Dependency Inversion** | Interfaces for services and repositories | Loose coupling, mock testing |
| **Open/Closed** | Extensible query parameters | Add filters without modifying core code |
| **Interface Segregation** | Specific DAO interfaces per table | Clean contracts, no unused methods |
| **Liskov Substitution** | All implementations replaceable | Consistent behavior across environments |

### 1.2 Architectural Patterns

```
┌─────────────────────────────────────────────────────────┐
│                   Client Applications                   │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP/REST
┌────────────────────────▼────────────────────────────────┐
│  Controller Layer (Request Validation & Response)       │
│  - @Valid annotations, DTO conversion                   │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Service Layer (Business Logic & Orchestration)         │
│  - Time range validation, data aggregation              │
│  - Pagination, sorting, caching strategy                │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│  Repository Layer (Database Access with JDBI)           │
│  - Custom SQL queries, result mapping                   │
│  - Connection pooling, statement caching                │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│              PostgreSQL Database                        │
│  - 4 aggregate tables with optimized indexes            │
└─────────────────────────────────────────────────────────┘
```

---

## 🏗️ 2. Module Structure & Organization

### 2.1 Package Architecture (Maven)

```
com.nimbo.restapi/
├── RestApiApplication.java          # Main class with @EnableJdbcRepositories
│
├── config/
│   ├── DataSourceConfig.java        # HikariCP connection pooling
│   ├── JdbiConfig.java              # JDBI configuration with RowMappers
│   ├── WebConfig.java               # CORS, interceptors, REST pagination
│   ├── OpenApiConfig.java           # Swagger/OpenAPI configuration
│   └── CacheConfig.java             # Caffeine cache configuration
│
├── controller/v1/                   # Versioned controllers
│   ├── TrafficController.java       # GET /api/v1/traffic
│   ├── ConnectionController.java    # GET /api/v1/connections
│   ├── ProtocolController.java      # GET /api/v1/protocols
│   └── BandwidthController.java     # GET /api/v1/bandwidth
│
├── dto/
│   ├── request/                     # Request DTOs with validation
│   │   ├── TimeRangeRequest.java    # time window parameters
│   │   ├── PaginationRequest.java   # page, size, sort
│   │   └── TrafficQueryRequest.java # composite query object
│   ├── response/
│   │   ├── ApiResponse.java         # Generic wrapper with metadata
│   │   ├── TrafficResponse.java
│   │   ├── ConnectionResponse.java
│   │   ├── ProtocolResponse.java
│   │   └── BandwidthResponse.java
│   └── mapper/                      # MapStruct interfaces
│       ├── TrafficMapper.java
│       ├── ConnectionMapper.java
│       └── ProtocolMapper.java
│
├── service/
│   ├── TrafficQueryService.java     # Interface
│   ├── TrafficQueryServiceImpl.java # Implementation with caching
│   ├── ConnectionQueryService.java
│   ├── ProtocolQueryService.java
│   └── BandwidthQueryService.java
│
├── repository/
│   ├── TrafficAggregateDao.java     # JDBI DAO interface
│   ├── ConnectionAggregateDao.java
│   ├── ProtocolUsageDao.java
│   └── IpBandwidthDao.java
│
├── model/                           # Domain models (not DTOs)
│   ├── TrafficAggregate.java        # JDBI result mapping
│   ├── ConnectionAggregate.java
│   └── ProtocolUsage.java
│
├── exception/
│   ├── ApiException.java            # Base custom exception
│   ├── InvalidTimeRangeException.java
│   ├── ResourceNotFoundException.java
│   ├── ValidationException.java
│   └── GlobalExceptionHandler.java  # @ControllerAdvice
│
├── validation/
│   ├── ValidTimeRange.java          # Custom annotation
│   ├── TimeRangeValidator.java
│   ├── ValidIpAddress.java
│   └── IpAddressValidator.java
│
└── util/
    ├── DateTimeUtils.java           # ISO 8601 parsing/formatting
    ├── IpUtils.java                 # IP validation, CIDR handling
    └── QueryMetrics.java            # Performance metrics collector
```

---

## ⚙️ 3. Core Components Requirements

### 3.1 Controller Layer Requirements

**Responsibilities:**
- Handle HTTP endpoints and request/response mapping
- Validate incoming requests using @Valid annotations
- Convert between DTOs and domain objects
- Delegate business logic to service layer
- Return consistent API responses with metadata

**Key Requirements:**
- Versioned endpoints (/api/v1/) for future compatibility
- Comprehensive OpenAPI/Swagger annotations for API documentation
- Consistent response wrapper with pagination metadata
- HTTP status codes mapping to business outcomes
- Input validation before entering service layer
- Extract authentication/authorization context if needed

**Endpoint Design:**
- GET /api/v1/traffic/{srcIp} - Get traffic for specific source IP
- GET /api/v1/traffic - List traffic with filters (time range, pagination)
- GET /api/v1/connections/{ip} - Get connections for an IP
- GET /api/v1/connections - List connections with filters
- GET /api/v1/protocols - Get protocol usage statistics
- GET /api/v1/bandwidth/top - Get top IPs by bandwidth
- GET /api/v1/bandwidth/{ip} - Get bandwidth for specific IP
- GET /api/v1/health - Health check endpoint

---

### 3.2 Service Layer Requirements

**Responsibilities:**
- Implement business logic and orchestration
- Validate time ranges (max 7 days, not future dates)
- Apply caching strategy for frequently queried data
- Handle pagination and sorting
- Aggregate data from multiple sources if needed

**Key Requirements:**
- **Caching Strategy:** Use Caffeine cache for frequently queried IPs with 5-minute TTL
- **Bulk Operations:** Batch fetch when possible for performance
- **Circuit Breaker:** Implement Resilience4j for database failure handling
- **Retry Logic:** Apply exponential backoff for transient errors
- **Time Range Validation:** Maximum 7 days, cannot query future dates
- **Data Aggregation:** Sum/aggregate data across time windows when needed
- **DTO Mapping:** Convert domain models to response DTOs
- **Metrics Collection:** Track query performance and execution times

**Service Methods Required:**
- `queryTrafficByIp(ip, timeRange, pagination)` - Traffic data for specific IP
- `queryTrafficWithFilters(queryRequest)` - Traffic with multiple filters
- `queryConnectionsByIp(ip, timeRange, pagination)` - Connection data for IP
- `queryProtocolUsage(timeRange)` - Protocol usage statistics
- `getTopBandwidthIPs(limit, timeRange)` - Top IPs by bandwidth
- `getBandwidthByIp(ip, timeRange)` - Bandwidth for specific IP

---

### 3.3 Repository Layer Requirements (JDBI)

**Responsibilities:**
- Database access with JDBI DAO interfaces
- Define SQL queries for each table
- Handle result mapping to domain models
- Implement paginated queries with count for metadata

**Key Requirements:**
- **RowMapper:** Consistent mapping between database results and domain models
- **Paginated queries:** Support limit/offset with count queries for metadata
- **Index Usage:** Ensure queries hit appropriate composite indexes
- **Prepared Statements:** Use JDBI parameter binding to prevent SQL injection
- **Query Optimization:** Use appropriate indexes and avoid functions on indexed columns

**Required DAOs and Queries:**

**TrafficAggregateDao:**
- `findByIpAndTimeRange(ip, startTime, endTime, limit, offset)` - Primary query
- `countByIpAndTimeRange(ip, startTime, endTime)` - Count for pagination
- `findByTimeRange(startTime, endTime, limit, offset)` - All traffic in time range
- `findTopTrafficIPs(startTime, endTime, limit)` - Top IPs by traffic volume

**ConnectionAggregateDao:**
- `findByIpAndTimeRange(ip, startTime, endTime, limit, offset)` - IP connections
- `findBySrcIpAndTimeRange(srcIp, startTime, endTime, limit, offset)` - Source IP lookups
- `findByDstIpAndTimeRange(dstIp, startTime, endTime, limit, offset)` - Destination IP lookups
- `countByIpAndTimeRange(ip, startTime, endTime)` - Count for pagination

**ProtocolUsageDao:**
- `findProtocolUsageByTimeRange(startTime, endTime)` - Protocol usage statistics
- `findByProtocolAndTimeRange(protocol, startTime, endTime)` - Specific protocol usage
- `getTopProtocols(startTime, endTime, limit)` - Top protocols by usage

**IpBandwidthDao:**
- `getTopBandwidthIPs(startTime, endTime, limit)` - Top IPs by bandwidth
- `findByIpAndTimeRange(ip, startTime, endTime)` - Bandwidth for specific IP
- `getTotalBandwidthByIp(ip, startTime, endTime)` - Aggregated bandwidth

---

## 🎨 4. Query Optimization Strategy

### 4.1 Index Strategy (Critical for Big Data)

| Table | Index | Type | Justification |
|-------|-------|------|---------------|
| traffic_aggregates | `(src_ip, window_start)` | B-Tree | Primary query pattern |
| connection_aggregates | `(src_ip, window_start)` | B-Tree | Source IP lookups |
| connection_aggregates | `(dst_ip, window_start)` | B-Tree | Destination IP lookups |
| protocol_usage_aggregates | `(l7_protocol, window_start)` | B-Tree | Protocol time-series queries |
| ip_bandwidth_aggregates | `(ip, window_start)` | B-Tree | IP bandwidth queries |
| **ALL TABLES** | `(window_start)` | B-Tree | Time range filters |
| **ALL TABLES** | `(batch_id)` | B-Tree | Audit and debugging |

### 4.2 Query Performance Requirements

**Optimization Rules:**
- All queries MUST use indexes for filtering and sorting
- Avoid functions on indexed columns in WHERE clauses
- Use proper time range queries (>= and <) instead of DATE() functions
- Use CTEs for complex queries to improve readability and performance
- Implement query timeout at service and repository levels (30 seconds max)
- Use pagination to limit result sets
- Consider partial indexes for frequently queried time ranges

**Required Query Patterns:**
- Primary filter: Primary Key column + window_start range
- Sorting: window_start DESC for time-series data
- Pagination: LIMIT and OFFSET with proper ordering
- Aggregation: Use SUM/COUNT with GROUP BY for summary statistics
- Top queries: ORDER BY total_bytes DESC LIMIT N

---

## 🛡️ 5. Error Handling & Validation Requirements

### 5.1 Custom Exception Hierarchy

**Exception Types Required:**
- **ApiException:** Base exception with HTTP status and error code
- **InvalidTimeRangeException:** Max range 7 days, cannot query future dates
- **ResourceNotFoundException:** No data found for given criteria
- **ValidationException:** Invalid IP format, pagination out of bounds
- **DatabaseException:** Database connectivity or query execution errors
- **RateLimitException:** Rate limiting exceeded

### 5.2 Global Exception Handler Requirements

**Responsibility:** Centralized exception handling across all controllers

**Key Requirements:**
- Log all exceptions with appropriate levels (WARN for client errors, ERROR for server errors)
- Return consistent ErrorResponse format with HTTP status, error code, and message
- Handle validation errors from @Valid annotations
- Sanitize error messages to prevent information leakage
- Provide meaningful error messages for debugging
- Track error metrics for monitoring

**Error Response Format:**
```json
{
  "status": "ERROR",
  "code": "TIME_RANGE_INVALID",
  "message": "Time range exceeds maximum allowed 7 days",
  "timestamp": "2026-08-28T14:00:00Z",
  "path": "/api/v1/traffic/192.168.1.1"
}
```

---

## 📊 6. Performance & Scaling Requirements

### 6.1 Caching Strategy

**Cache Configuration:**
- **traffic cache:** TTL 5 minutes, max 10,000 entries, LRU eviction
- **connection cache:** TTL 5 minutes, max 5,000 entries, LRU eviction
- **protocol cache:** TTL 1 hour, max 100 entries (rarely changes)
- **bandwidth cache:** TTL 5 minutes, max 5,000 entries, LRU eviction
- Cache key strategy: Combine all query parameters for uniqueness
- Cache eviction: Time-based and size-based with LRU

### 6.2 Connection Pooling Requirements (HikariCP)

**Pool Configuration:**
- Maximum pool size: 20 connections
- Minimum idle connections: 10
- Idle timeout: 300,000ms (5 minutes)
- Connection timeout: 30,000ms
- Max lifetime: 1,800,000ms (30 minutes)
- Leak detection threshold: 2,000ms (detect slow queries)
- Connection test query: SELECT 1

### 6.3 Query Timeout Requirements

**Timeout Configuration:**
- Service layer: 30 seconds max per transaction
- Repository layer: 30 seconds per query
- Slow query detection: Log queries > 1 second
- Circuit breaker timeout: 15 seconds
- Retry logic: 3 retries with exponential backoff (100ms, 200ms, 400ms)

---

## 🧪 7. Testing Requirements

### 7.1 Test Pyramid Distribution

| Test Type | Coverage | Tools | Requirements |
|-----------|----------|-------|--------------|
| **Unit Tests** | 75% | JUnit 5, Mockito, AssertJ | Mock all dependencies, test business logic |
| **Integration Tests** | 20% | TestContainers, Spring Boot Test | Use real database container, test DAOs |
| **E2E Tests** | 5% | Spring Boot Test, TestRestTemplate | Test full request/response flow |

### 7.2 Test Requirements

**Unit Tests:**
- Service layer: Mock DAOs, test business logic and validation
- Controllers: Mock services, test request/response mapping
- Mappers: Test DTO to domain model conversion
- Validation: Test custom validation annotations
- Exception handling: Test error scenarios

**Integration Tests:**
- Repository layer: Test SQL queries against real PostgreSQL
- Database connectivity: Test connection pool configuration
- Cache integration: Test caching behavior
- Transaction management: Test timeout configuration

**Performance Tests:**
- Load tests: 500 concurrent requests
- Stress tests: Peak load handling
- Endurance tests: Extended runtime stability
- Query performance: p95 < 200ms, p99 < 500ms

---

## 🔒 8. Security Requirements

| Aspect | Requirement | Implementation |
|--------|-------------|----------------|
| **Authentication** | JWT token validation | Spring Security + JWT |
| **Authorization** | Role-based access control | Method-level @PreAuthorize |
| **Rate Limiting** | 100 requests per minute per IP | Bucket4j implementation |
| **Audit Logging** | Log all query parameters | Structured logging with masked IPs |
| **Data Masking** | Never log full IPs | Mask last octet (192.168.1.xxx) |
| **SQL Injection** | Prevent injection attacks | JDBI prepared statements |
| **CORS** | Configure cross-origin policies | Spring WebConfig |
| **SSL/TLS** | HTTPS for all connections | Configure in application.yml |
| **Input Validation** | Validate all request parameters | Custom validators |

---

## 📈 9. Monitoring & Observability Requirements

### 9.1 Metrics Requirements

**Required Metrics:**
- `api.query.duration`: Timer tracking response time per endpoint
- `api.query.count`: Counter for request volume
- `api.query.error`: Counter for error rates
- `jdbc.connections.active`: Gauge for connection pool health
- `cache.hit.ratio`: Gauge for cache effectiveness
- `db.query.slow`: Counter for queries > 1 second
- `db.connection.timeout`: Counter for connection timeout errors
- `api.rate.limit`: Counter for rate limit exceeded events

### 9.2 Logging Requirements

**Log Levels:**
- com.nimbo.restapi: INFO in production, DEBUG in development
- org.springframework.jdbc: WARN
- org.jdbi: WARN

**Structured Logging:**
- Use JSON format in production
- Include correlation IDs for tracing
- Log request IDs for debugging
- Mask sensitive data (IPs)

**Required Log Events:**
- Query execution times
- Cache hit/miss events
- Exception occurrences
- Rate limiting events
- Database connection events

---

## 🚀 10. Deployment & Configuration Requirements

### 10.1 Environment Configuration

| Environment | Profile | Database | Cache | Log Level | Actuator |
|-------------|---------|----------|-------|-----------|----------|
| **Development** | `dev` | Local PostgreSQL | Disabled | DEBUG | Full access |
| **Testing** | `test` | TestContainers | Disabled | INFO | Full access |
| **Staging** | `staging` | Staging DB | TTL 5min | INFO | Limited |
| **Production** | `prod` | Production DB | TTL 5min | WARN | Minimal |

### 10.2 Application Requirements

**Configuration Management:**
- Environment variables for sensitive data (DB credentials, API keys)
- Profile-specific configuration files
- External configuration via Spring Cloud Config (if applicable)
- Configuration validation on startup

**Required Configurations:**
- Database connection URL, username, password
- HikariCP pool settings
- Cache specifications
- Pagination defaults
- Time range limits
- Actuator endpoints
- Logging configuration

---

## 📅 11. Development Timeline (Estimates)

| Phase | Tasks | Duration | Deliverables |
|-------|-------|----------|--------------|
| **Sprint 1** | Project setup, configs, base classes | 3 days | Working Spring Boot project |
| **Sprint 2** | Repository layer for all 4 tables | 5 days | DAOs with CRUD queries |
| **Sprint 3** | Service layer + caching | 5 days | Business logic + unit tests |
| **Sprint 4** | Controller layer + DTOs + mappers | 4 days | REST endpoints with OpenAPI |
| **Sprint 5** | Exception handling + validation | 3 days | Global error handling |
| **Sprint 6** | Performance optimization + testing | 5 days | Load tests, query tuning |
| **Sprint 7** | Security + monitoring + deployment | 5 days | Production-ready module |

**Total: ~30 working days** (6 weeks)

---

## 🔧 12. Technical Debt Prevention

### Code Quality Gates
- **SonarQube:** Minimum 80% coverage, no critical issues
- **SpotBugs:** Zero bugs or vulnerabilities
- **Checkstyle:** Consistent Google Java Style
- **OWASP Dependency Check:** No known vulnerabilities

### Documentation Requirements
- **README.md:** Quick start guide with setup instructions
- **OpenAPI:** Complete API documentation with examples
- **Architecture Decision Records (ADR):** For major technical decisions
- **Wiki:** Development setup, deployment guide, troubleshooting

### Code Review Requirements
- Mandatory code reviews for all PRs
- At least 2 approvals before merge
- Run all tests before merge
- Pass all quality gates before merge

---

## 🎯 13. Success Criteria

| Aspect | Metric | Target | Measurement |
|--------|--------|--------|-------------|
| **Performance** | p95 latency | < 200ms for 7-day queries | Load tests |
| **Performance** | p99 latency | < 500ms for 7-day queries | Load tests |
| **Throughput** | QPS | 500 req/sec per instance | Load tests |
| **Availability** | Uptime | 99.95% (4.38 hours/year downtime) | Production monitoring |
| **Data Freshness** | Max latency | < 5 minutes (batch lag) | Batch monitoring |
| **Test Coverage** | Unit + Integration | > 80% | SonarQube |
| **Code Quality** | SonarQube | A-grade | SonarQube |
| **Error Rate** | API errors | < 0.1% | Production monitoring |
| **Cache Hit Rate** | Cache efficiency | > 80% | Metrics monitoring |

---

## 🎬 14. Immediate Next Steps

1. **Setup repository structure** with Maven multi-module
2. **Configure database connection** with proper credentials
3. **Create base entity classes** for 4 aggregate tables
4. **Implement JDBI DAOs** with sample queries
5. **Write migration scripts** for indexes (if missing)
6. **Initial service layer** with basic query logic
7. **Create controllers** for first endpoint (`/traffic/{srcIp}`)
8. **Add integration tests** with Testcontainers

---

## 📚 15. References & Best Practices Followed

- **Clean Architecture** by Robert C. Martin
- **Domain-Driven Design** (Tactical Patterns)
- **Spring Boot Best Practices** by Pivotal
- **PostgreSQL Performance Tuning**
- **Google SRE Book** (Service Level Objectives)
- **12-Factor App** for cloud-native design

---

## 💡 Additional Recommendations

1. **Consider TimescaleDB:** If time-series queries become heavy, TimescaleDB hypertables would be perfect for these aggregate tables.

2. **Read Replica:** For production, consider a separate read replica to offload queries from the writer database.

3. **Materialized Views:** For very frequent queries (e.g., top bandwidth by day), create materialized views refreshed by the batch processor.

4. **GraphQL Option:** If your API consumers need flexible querying, consider adding a GraphQL layer alongside REST.

5. **CQRS Eventual Consistency:** Since this is read-only, ensure consumers understand data might be up to 5 minutes behind real-time.

6. **Connection Pool Monitoring:** Implement metrics for HikariCP to track pool utilization and prevent exhaustion.

7. **Query Plan Analysis:** Regularly analyze slow query logs and optimize with EXPLAIN ANALYZE.

8. **Backup Strategy:** Ensure proper backup of aggregate tables since they contain processed data.

---

**This high-level plan is ready for your review. Once approved, I can provide detailed implementation specifications for any specific component.**

Would you like me to:
1. **Elaborate on any specific component** requirements (e.g., full DAO specification, controller with all filters)?
2. **Provide detailed specifications** for a specific module?
3. **Write the database migration scripts** for indexes?
4. **Create the OpenAPI/Swagger specification** in YAML?
5. **Design the caching strategy** in more detail with key patterns?

Let me know your priority! 🚀