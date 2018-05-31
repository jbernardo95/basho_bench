-module(basho_bench_riakclient_keygen).

-export([generate/2]).

-define(BASHO_BENCH_BUCKET, <<"basho_bench_bucket">>).

generate(WorkerId, MaxKey) ->
    fun(Operation) ->
        do_generate(Operation, WorkerId, MaxKey)
    end.

do_generate({populate, undefined}, WorkerId, NKeys) ->
    do_generate({populate, -1}, WorkerId, NKeys);
do_generate({populate, LastKeyBin}, WorkerId, NKeys) when is_binary(LastKeyBin) ->
    LastKeyInt = bin_bigendian_to_int(LastKeyBin),
    do_generate({populate, LastKeyInt}, WorkerId, NKeys);
do_generate({populate, LastKey}, WorkerId, NKeys) when is_integer(LastKey) ->
    Nodes = basho_bench_config:get(riakclient_nodes),
    Node = lists:nth(((WorkerId - 1) rem length(Nodes)) + 1, Nodes),

    NKeysPerNode = round(NKeys / length(Nodes)),
    Key = LastKey + 1,
    if
        Key > NKeysPerNode -> max_key_reached;
        true -> {Node, ?BASHO_BENCH_BUCKET, int_to_bin_bigendian(Key)}
    end;

do_generate(leaf_tx_manager_transaction, WorkerId, NKeys) ->
    Nodes = basho_bench_config:get(riakclient_nodes),
    Node = lists:nth(((WorkerId - 1) rem length(Nodes)) + 1, Nodes),

    NWorkers = basho_bench_config:get(concurrent),
    NWorkersPerNode = round(NWorkers / length(Nodes)),
    NKeysPerNode = round(NKeys / length(Nodes)),
    NKeysPerWorkerPerNode = round(NKeysPerNode / NWorkersPerNode),
    RandomInt = random:uniform(NKeysPerWorkerPerNode) - 1,
    Key = ((WorkerId rem NWorkersPerNode) + 1) + (NWorkersPerNode * RandomInt),

    {Node, ?BASHO_BENCH_BUCKET, int_to_bin_bigendian(Key)};

do_generate(root_tx_manager_transaction, WorkerId, NKeys) ->
    Nodes = basho_bench_config:get(riakclient_nodes),
    Node1 = lists:nth(((WorkerId - 1) rem length(Nodes)) + 1, Nodes),
    Node2 = lists:nth((((WorkerId + 1) - 1) rem length(Nodes)) + 1, Nodes),

    NWorkers = basho_bench_config:get(concurrent),
    NWorkersPerNode = round((NWorkers * 2) / length(Nodes)),
    NKeysPerNode = round(NKeys / length(Nodes)),
    NKeysPerWorkerPerNode = round(NKeysPerNode / NWorkersPerNode),
    RandomInt1 = random:uniform(NKeysPerWorkerPerNode) - 1,
    RandomInt2 = random:uniform(NKeysPerWorkerPerNode) - 1,
    Key1 = ((WorkerId rem NWorkersPerNode) + 1) + (NWorkersPerNode * RandomInt1),
    Key2 = ((WorkerId rem NWorkersPerNode) + 1) + (NWorkersPerNode * RandomInt2),

    [{Node1, ?BASHO_BENCH_BUCKET, int_to_bin_bigendian(Key1)},
     {Node2, ?BASHO_BENCH_BUCKET, int_to_bin_bigendian(Key2)}].

bin_bigendian_to_int(Bin) ->
    <<N:32/big>> = Bin,
    N.
int_to_bin_bigendian(N) ->
    <<N:32/big>>.
