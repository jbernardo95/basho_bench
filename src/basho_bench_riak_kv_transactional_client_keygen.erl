-module(basho_bench_riak_kv_transactional_client_keygen).

-export([function/1]).

-define(BASHO_BENCH_BUCKET, <<"basho_bench_bucket">>).

function(WorkerId) ->
    fun(TransactionType) ->
        do_generate(TransactionType, WorkerId)
    end.

do_generate({populate, undefined}, WorkerId) ->
    do_generate({populate, 0}, WorkerId);
do_generate({populate, LastKeyBin}, WorkerId) when is_binary(LastKeyBin) ->
    LastKeyInt = bin_bigendian_to_int(LastKeyBin),
    do_generate({populate, LastKeyInt}, WorkerId);
do_generate({populate, LastKey}, _WorkerId) when is_integer(LastKey) ->
    RiakNodes = basho_bench_config:get(riak_nodes),
    NKeys = basho_bench_config:get(n_keys),

    Key = LastKey + 1,
    Node = lists:nth(((Key - 1) rem length(RiakNodes)) + 1, RiakNodes),
    Bucket = int_to_bin_bigendian(1),
    KeyBin = int_to_bin_bigendian(Key),
    if
        Key > NKeys -> max_key_reached;
        true -> [{Node, Bucket, KeyBin}]
    end;

do_generate(local, WorkerId) ->
    NWorkers = basho_bench_config:get(concurrent),
    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    NKeys = basho_bench_config:get(n_keys),

    NOperationsPerTransaction = lists:foldl(fun({_Operation, N}, Acc) -> N + Acc end, 0, OperationsPerTransaction),
    WorkerNodes = worker_nodes(WorkerId),
    WorkerBuckets = worker_buckets(WorkerId, NOperationsPerTransaction),
    NKeysPerWorkerPerNode = round((NKeys / NWorkers) / length(WorkerNodes)),
    lists:map(fun(I) ->
                  Node = lists:nth(1, WorkerNodes),
                  Bucket = lists:nth(I, WorkerBuckets),
                  Key = random:uniform(NKeysPerWorkerPerNode),
                  KeyBin = int_to_bin_bigendian(Key),
                  {Node, Bucket, KeyBin}
              end, lists:seq(1, NOperationsPerTransaction));

do_generate(distributed, _WorkerId) ->
    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    NKeys = basho_bench_config:get(n_keys),
    RiakNodes = basho_bench_config:get(riak_nodes),

    NOperationsPerTransaction = lists:foldl(fun({_Operation, N}, Acc) -> N + Acc end, 0, OperationsPerTransaction),
    lists:map(fun(_I) ->
                  Key = random:uniform(NKeys),
                  Node = lists:nth(((Key - 1) rem length(RiakNodes)) + 1, RiakNodes),
                  Bucket = int_to_bin_bigendian(1),
                  KeyBin = int_to_bin_bigendian(Key),
                  {Node, Bucket, KeyBin}
              end, lists:seq(1, NOperationsPerTransaction)).

% Returns a list of nodes that a given worker can access according to the n_nodes_per_worker config parameter
worker_nodes(WorkerId) ->
    Nodes = basho_bench_config:get(riak_nodes),
    NNodesPerWorker = basho_bench_config:get(n_nodes_per_worker, 2),
    lists:map(fun(I) ->
                  lists:nth((((WorkerId - 1) + I) rem length(Nodes)) + 1, Nodes)
              end, lists:seq(0, (NNodesPerWorker - 1))).

% Returns a list of buckets that a given worker can access according to the contention config parameter
worker_buckets(WorkerId, NOperations) ->
    Concurrent = basho_bench_config:get(concurrent),
    NBashoBenchNodes = basho_bench_config:get(n_basho_bench_nodes),
    NWorkers = Concurrent * NBashoBenchNodes,
    Contention = basho_bench_config:get(contention, 0),
    Buckets1 = [int_to_bin_bigendian(WorkerId) || _I <- lists:seq(1, NOperations - Contention)],
    Buckets2 = lists:map(fun(I) ->
                            int_to_bin_bigendian((((WorkerId - 1) + I) rem NWorkers) + 1)
                         end, lists:seq(1, Contention)),
    Buckets1 ++ Buckets2.

bin_bigendian_to_int(Bin) ->
    <<N:32/big>> = Bin,
    N.
int_to_bin_bigendian(N) ->
    <<N:32/big>>.
