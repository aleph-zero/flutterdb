
### Issues

1. Membership Service: We didn't define an interface to inject into the handler. Does it matter? 
2. Add implicit cast for when a binary operator has both an integer and floating point operand
3. Consider refactoring the `membership` service to use the pattern outlined below.
4. Implement render/bind pattern for all APIs.
5. Do we ever use the symbol table returned by engine.ResolveSymbols()?
6. Clean up client logic. We can run `flutterdb client` and it will start even though it does not know where to connect to
7. OTEL: Looks like the implementation is such that the client actually connects to an OTEL collector. Need to refactor so that it does not need this and only propagates trace context via headers.
8. Symbol resolution tests non-deterministically fail. Fix
10. For SELECT statements with no table, for instance 'SELECT 1', we can insert a DummyTable struct type to make query planning and processing easier.
11. Constant expression optimization does not work in SELECT predicates with AND/OR/NOT
12. Add unique query ID to each query so we can track via logs
13. Physical plan operators must have a close() method in order to shut down the channels and stop the go routines
14. Fun project: Add operator statistics for each operator type and export as OTEL metrics
15. Why do we have a 10 second timeout in server.Bootstrap()? ctx, shutdown := context.WithTimeout(context.Background(), 10*time.Second)
16. Refactor logical plan construction to use the visitor pattern that we use in the physical plan construction so that we are not in a mess of if/else statements.
17. Should engine.HitCollector be moved into package index and out of package engine?
18. Projection on expressions does not work. This statement: "SELECT 1+2" does not work. 

### Running with OpenTelemetry
Before running the server set the address of the collector:

```export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318```

### Service Provider Implementations

#### Basic Pattern 
First define a service interface
```
type Service interface {
  Open() error
}
```

Then define a concrete struct implementation that implements the interface
```
type ServiceProvider struct {}

func (sp *ServiceProvider) Open() error {
    return nil
}
```

Then define a function to create a new concrete implementation, and return the interface
```
func NewService() Service {
    return &ServiceProvider{}
}
```

This way you can send back a pointer to an implementation, but any functions/methods 
that need the service will be declared to take the interface. 

DO NOT DECLARE METHODS THAT NEED THE SERVICE TO ACCEPT A POINTER TO THE INTERFACE, 
OTHERWISE YOU WILL NOT BE ABLE TO CALL THE METHODS OF THE IMPLEMENTATION
