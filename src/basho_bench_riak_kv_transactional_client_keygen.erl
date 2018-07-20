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
do_generate({populate, LastKey}, WorkerId) when is_integer(LastKey) ->
    NWorkers = basho_bench_config:get(concurrent),
    NKeys = basho_bench_config:get(n_keys),

    Nodes = worker_nodes(WorkerId),
    Bucket = int_to_bin_bigendian(WorkerId),
    Key = LastKey + 1,
    KeyBin = int_to_bin_bigendian(Key),
    NKeysPerWorkerPerNode = round((NKeys / NWorkers) / length(Nodes)),
    if
        Key > NKeysPerWorkerPerNode -> max_key_reached;
        true -> lists:map(fun(Node) -> {Node, Bucket, KeyBin} end, Nodes)
    end;

do_generate(local, WorkerId) ->
    NWorkers = basho_bench_config:get(concurrent),
    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    NKeys = basho_bench_config:get(n_keys),

    NOperationsPerTransaction = lists:foldl(fun({_Operation, N}, Acc) -> N + Acc end, 0, OperationsPerTransaction),
    [Node | _] = Nodes = worker_nodes(WorkerId),
    NKeysPerWorkerPerNode = round((NKeys / NWorkers) / length(Nodes)),
    Bucket = int_to_bin_bigendian(WorkerId),
    lists:map(fun(_I) ->
                  Key = random:uniform(NKeysPerWorkerPerNode),
                  KeyBin = int_to_bin_bigendian(Key),
                  {Node, Bucket, KeyBin}
              end, lists:seq(1, NOperationsPerTransaction));

do_generate(distributed, WorkerId) ->
    NWorkers = basho_bench_config:get(concurrent),
    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    NKeys = basho_bench_config:get(n_keys),

    NOperationsPerTransaction = lists:foldl(fun({_Operation, N}, Acc) -> N + Acc end, 0, OperationsPerTransaction),
    Nodes = worker_nodes(WorkerId),
    NKeysPerWorkerPerNode = round((NKeys / NWorkers) / length(Nodes)),
    Bucket = int_to_bin_bigendian(WorkerId),
    lists:map(fun(I) ->
                  Node = lists:nth(((I - 1) rem length(Nodes)) + 1, Nodes),
                  Key = random:uniform(NKeysPerWorkerPerNode),
                  KeyBin = int_to_bin_bigendian(Key),
                  {Node, Bucket, KeyBin}
              end, lists:seq(1, NOperationsPerTransaction)).

worker_nodes(WorkerId) ->
    Nodes = basho_bench_config:get(riak_nodes),
    NNodesPerWorker = basho_bench_config:get(n_nodes_per_worker, 2),
    lists:map(fun(I) ->
                  lists:nth((((WorkerId - 1) + (I - 1)) rem length(Nodes)) + 1, Nodes)
              end, lists:seq(1, NNodesPerWorker)).

bin_bigendian_to_int(Bin) ->
    <<N:32/big>> = Bin,
    N.
int_to_bin_bigendian(N) ->
    <<N:32/big>>.
