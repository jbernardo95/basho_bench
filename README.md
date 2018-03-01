# basho_bench

This is a fork of [basho_bench](https://github.com/basho/basho_bench) altered to work with [this](https://github.com/jbernardo95/riak_kv) fork of `riak_kv`.

## Documentation

Documentation for this project can be found [here](https://docs.basho.com/riak/kv/2.2.3/using/performance/benchmarking/).

## Dependencies

- Erlang R16B02
- GNU-style build system

## Compilation 

```
# Install dependencies, compile and build release
$ make rel

# Clean build files
$ make relclean
```

## Running Experiments

```
# In a client node
$ cd /basho_bench

# Configure experiment parameters in etc/riakclient.config

# Run experiment
$ bin/basho_bench -N bb@<ip> \
                  -C riak \
                  -d /basho_bench/tests \
                  ../../../etc/riakclient.config
```
