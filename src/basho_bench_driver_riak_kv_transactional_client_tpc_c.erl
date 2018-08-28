-module(basho_bench_driver_riak_kv_transactional_client_tpc_c).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {id, riak_nodes, client}).

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

    RiakNodes = basho_bench_config:get(riak_nodes),
    Cookie = basho_bench_config:get(riak_cookie, 'riak'),
    establish_connection_to_nodes(RiakNodes, Cookie),

    global:sync(),

    TargetNode = lists:nth(((Id - 1) rem length(RiakNodes)) + 1, RiakNodes),
    case riak_kv_transactional_client:start_link(TargetNode) of
        {ok, Client} ->
            % Hack to init the client whith a snapshot equal to the latest one
            LatestSnapshotNode = lists:nth(1, RiakNodes),
            LatestSnapshot = term_to_binary(latest_snapshot),
            riak_kv_transactional_client:get(LatestSnapshotNode, LatestSnapshot, LatestSnapshot, Client),

            State = #state{id = Id,
                           riak_nodes = RiakNodes,
                           client = Client},
            {ok, State};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start riak_kv_transactional_client: ~p\n", [Reason2])
    end.

run(populate, _, _, #state{id = Id, riak_nodes = RiakNodes, client = Client}) ->
    ok = tpc_c_populate:populate(Id, RiakNodes, Client),
    {stop, population_finished};

run(TransactionType, _, _, #state{id = Id, riak_nodes = RiakNodes, client = Client} = State) ->
    case TransactionType of
        new_order -> tpc_c_new_order_transaction_fsm:start_link(Id, RiakNodes, Client);
        payment -> tpc_c_payment_transaction_fsm:start_link(Id, RiakNodes, Client);
        order_status -> tpc_c_order_status_transaction_fsm:start_link(Id, RiakNodes, Client);
        stock_level -> tpc_c_stock_level_transaction_fsm:start_link(Id, RiakNodes, Client)
    end,

    receive
        ok -> {ok, State};
        {error, Reason} -> {error, Reason, State}
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
