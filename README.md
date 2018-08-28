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

## TPC-C

This repo contains an implementation of TPC-C version 5.11.0 according to the [official spec](http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp). It does not include an implementation of the delivery transaction.

This implementation of TPC-C is built to work with an [altered version](https://github.com/jbernardo95/riak_kv/tree/tree) of Riak KV. It assumes that there is a warehouse per Riak node. 

Implementation details specific to Riak KV/key-value stores:

- The data of each table is stored in what Riak calls buckets, with the key equal to the table's primary key and the value equal to the row's data.
- An extra bucket called `customers_last_name_index` is used to index customers by their last name, so that queries of customers by their last are possible (requirement of the payment and order-status transactions).
- An extra bucket called `last_customer_orders` is used to store duplicate versions of each client's latest order. This bucket was added to allow clients to query the latest order of a client, as this is requirement of the order-status transaction.


future work
improve single attribute updates by keeping them separate in separate keys suffixed with the keyname
keep a copy of the items table in every node (items table is read only so this is possible)
implement tests described in chapter 3



1 warehouse per riak node
10 clients per warehouse

to populate create one populate client per node

new-order 45%
payment 43%
order-status 6%
stock-level 6%
delivery 0% (not implemented)

what test time should we use ?
should we implement the delivery transaction ? how ?






continuar

o porblema atual éas versões que estão a desaparecer muito rapido por terem um limite baixo e not_found começam a aparecer onde não deviam

para resolver isto temos de aumentar o numero maximo de versoes por objecto
aumentar o limite de versoes, para que os not_found nao acontençam

mas os not_found eventualemnte vao aparecer e isto tem de ser considerado e handled no codigo
quando isto acontece devemos abortar a transação porque o que ela quer ler já foi garbage collected (dar track disto, porque se isto aconceter muito pode projedicar a performance e a unica solução é aumentar o numero de versoes mantidas)
