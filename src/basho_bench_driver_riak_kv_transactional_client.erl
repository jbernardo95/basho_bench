-module(basho_bench_driver_riak_kv_transactional_client).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {id, client, populate_last_key}).

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

    TargetNode = lists:nth(((Id - 1) rem length(Nodes)) + 1, Nodes),
    case riak_kv_transactional_client:start_link(TargetNode) of
        {ok, Client} ->
            State = #state{id = Id,
                           client = Client,
                           populate_last_key = undefined},
            {ok, State};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start riak_kv_transactional_client: ~p\n", [Reason2])
    end.

run(
  populate,
  NodeBucketKeyGen,
  ValueGen,
  #state{id = Id,
         client = Client,
         populate_last_key = PopulateLastKey} = State
) ->
    case NodeBucketKeyGen({populate, PopulateLastKey}) of
        {Node, Bucket, Key} ->
            case riak_kv_transactional_client:put(Node, Bucket, Key, ValueGen(), Client) of
                ok ->
                    {ok, State#state{populate_last_key = Key}};
                {error, aborted} ->
                    {error, aborted, State}
            end;
        max_key_reached ->
            {stop, max_key_reached}
    end;

run(TransactionType, NodeBucketKeyGen, ValueGen, #state{client = Client} = State) ->
    Nbkeys = NodeBucketKeyGen(TransactionType),

    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    Operations = lists:foldl(fun({Operation, N}, Acc) ->
                                     [Operation || _I <- lists:seq(1, N)] ++ Acc
                             end, [], OperationsPerTransaction),

    riak_kv_transactional_client:begin_transaction(Client),

    lists:foldl(fun(Operation, I) ->
                    {Node, Bucket, Key} = lists:nth(I, Nbkeys),
                    case Operation of
                        get -> riak_kv_transactional_client:get(Node, Bucket, Key, Client);
                        put -> riak_kv_transactional_client:put(Node, Bucket, Key, ValueGen() ,Client)
                    end,
                    I + 1
                end, 1, Operations),

    case riak_kv_transactional_client:commit_transaction(Client) of
        ok ->
            {ok, State};
        {error, aborted}->
            {error, aborted, State}
    end.

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
