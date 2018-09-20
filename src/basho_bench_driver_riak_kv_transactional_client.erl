-module(basho_bench_driver_riak_kv_transactional_client).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {id, worker_target_throughput, populate_last_key}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    case code:which(riak_kv_transactional_client) of
        non_existing ->
            ?FAIL_MSG("~s requires riak_kv_transactional_client module to be available on the code path.\n", [?MODULE]);
        _ ->
            ok
    end,

    BashoBenchNode = basho_bench_config:get(basho_bench_node, [basho_bench, longnames]),
    case net_kernel:start(BashoBenchNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    Nodes = basho_bench_config:get(riak_nodes),
    Cookie = basho_bench_config:get(riak_cookie, 'riak'),
    establish_connection_to_nodes(Nodes, Cookie),

    global:sync(),

    TargetThroughput = basho_bench_config:get(target_throughput),
    NBashoBenchNodes = basho_bench_config:get(n_basho_bench_nodes),
    TargetThroughputPerBashoBenchNode = TargetThroughput / NBashoBenchNodes, 
    NWorkers = basho_bench_config:get(concurrent),
    WorkerTargetThroughput = TargetThroughputPerBashoBenchNode / NWorkers,

    TargetNode = lists:nth(((Id - 1) rem length(Nodes)) + 1, Nodes),

    State = #state{id = Id,
                   worker_target_throughput = WorkerTargetThroughput,
                   populate_last_key = undefined},
    {ok, State};

    case riak_kv_transactional_client:start_link(TargetNode) of
        {ok, Client} ->
            State = #state{id = Id,
                           populate_last_key = undefined},
            {ok, State};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start riak_kv_transactional_client: ~p\n", [Reason2])
    end.

run(
  populate,
  NodeBucketKeyGen,
  ValueGen,
  #state{client = Client,
         populate_last_key = PopulateLastKey} = State
) ->
    case NodeBucketKeyGen({populate, PopulateLastKey}) of
        max_key_reached ->
            {stop, max_key_reached};
        Nbkeys ->
            lists:foreach(fun({Node, Bucket, Key}) ->
                              ok = riak_kv_transactional_client:put(Node, Bucket, Key, ValueGen(), Client)
                          end, Nbkeys),
            [{_, _, Key} | _] = Nbkeys,
            {ok, State#state{populate_last_key = Key}}
    end;

run(TransactionType, NodeBucketKeyGen, ValueGen, #state{client = Client} = State) ->
    erlang:spawn_link(basho_bench_riak_kv_transactions_executor, execute_transaction, [TransactionType, NodeBucketKeyGen, ValueGen]),
    timer:sleep(X),
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

establish_connection_to_nodes([], _Cookie) ->
    ok;
establish_connection_to_nodes([Node | Rest], Cookie) ->
    erlang:set_cookie(Node, Cookie),
    case net_adm:ping(Node) of
        pong ->
            establish_connection_to_nodes(Rest, Cookie);
        pang ->
            ?FAIL_MSG("Failed to establish connection to node ~p\n", [Node])
    end.
