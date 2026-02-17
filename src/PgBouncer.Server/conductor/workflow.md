# Workflow

1.  **Analyze**: Review logs to identify the "PostgreSQL authentication failed" error patterns. [x]
2.  **Reproduce**: Create a `StressTester` tool to simulate high concurrency and trigger the auth failure. [x]
3.  **Fix**: Investigate the root cause (likely thread starvation, race condition, or backend overload handling) and apply a fix. [x]
    - Implemented retry logic in `ClientSession.GetOrAcquireBackendAsync` to handle transient auth failures.
4.  **Verify**: Run `StressTester` again to ensure stability. [x]
    - Result: 1000/1000 requests successful under high load.
