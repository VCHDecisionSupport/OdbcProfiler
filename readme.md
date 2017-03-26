# ODBC Profiler Web App

Automatically profile and visually review Sql Server and Denodo (ODBC driver) data sources
- Table Row Count
- Column Distinct Count
- Column Value Histogram (ie `GROUP BY`)

## Use Case

Centralized starting point for testing
- Quick and easy access to volumes for high level validation
- Compare equivalent tables across different servers
- Compare Denodo and Sql Server
- Monitor profiles to detect deviations 
- Drill into profiles to identify underlying causes

### Rules Based Monitoring

For each profile measure:

- Simple Moving Average
- Exponential Moving Average
- Variance