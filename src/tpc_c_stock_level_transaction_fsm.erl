-module(tpc_c_stock_level_transaction_fsm).

-behaviour(gen_fsm).

-include("tpc_c.hrl").

-export([start_link/3]).
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).
-export([begin_transaction/2,
         read_district/2,
         read_orders/2,
         read_order_lines/2,
         read_stocks/2,
         commit_transaction/2,
         abort/2]).

-record(state, {worker_id,
                riak_nodes,
                client,
                requester,
                warehouse_id,
                warehouse_node,
                district_id,
                district_next_order_id,
                order_lines,
                item_ids}).

%% ===================================================================
%% Public API
%% ===================================================================

start_link(WorkerId, RiakNodes, Client) ->
    start_link(WorkerId, RiakNodes, Client, self()).

start_link(WorkerId, RiakNodes, Client, Requester) ->
    gen_fsm:start_link(?MODULE, [WorkerId, RiakNodes, Client, Requester], []).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([WorkerId, RiakNodes, Client, Requester]) ->
    StateData = #state{worker_id = WorkerId,
                       riak_nodes = RiakNodes,
                       client = Client,
                       requester = Requester},
    {ok, begin_transaction, StateData, 0}.

begin_transaction(timeout, #state{client = Client} = StateData) ->
    ok = riak_kv_transactional_client:begin_transaction(Client),
    {next_state, read_district, StateData, 0}.

read_district(
  timeout,
  #state{worker_id = WorkerId,
         riak_nodes = RiakNodes,
         client = Client} = StateData
) ->
    RiakNodesLength = length(RiakNodes),

    WarehouseId = ((WorkerId - 1) rem RiakNodesLength) + 1,
    WarehouseNode = lists:nth(((WarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),
    DistrictId = ((WorkerId - 1) rem ?N_DISTRICTS_PER_WAREHOUSE) + 1,

    Key = term_to_binary({WarehouseId, DistrictId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?DISTRICTS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, DistrictObject} ->
            District = binary_to_term(riak_object:get_value(DistrictObject)),

            NewStateData = StateData#state{warehouse_id = WarehouseId,
                                           warehouse_node = WarehouseNode,
                                           district_id = DistrictId,
                                           district_next_order_id = District#district.d_next_o_id},
            {next_state, read_orders, NewStateData, 0}
    end.

read_orders(
  timeout,
  #state{district_next_order_id = DistrictNextOrderId} = StateData
) ->
    NewStateData = StateData#state{order_lines = []},
    do_read_orders(DistrictNextOrderId - 21, NewStateData).

do_read_orders(
  OrderId,
  #state{district_next_order_id = DistrictNextOrderId} = StateData
) when OrderId == DistrictNextOrderId ->
    {next_state, read_order_lines, StateData, 0};
do_read_orders(
  OrderId,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_lines = OrderLines} = StateData
) ->
    Key = term_to_binary({WarehouseId, DistrictId, OrderId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?ORDERS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, OrderObject} ->
            Order = binary_to_term(riak_object:get_value(OrderObject)),

            NewOrderLines = lists:foldl(fun(OrderLineNumber, Acc) ->
                                                [{OrderId, OrderLineNumber} | Acc]
                                        end, OrderLines, lists:seq(1, Order#order.o_ol_cnt)),
            NewStateData = StateData#state{order_lines = NewOrderLines},
            do_read_orders(OrderId + 1, NewStateData)
    end.

read_order_lines(timeout, StateData) ->
    NewStateData = StateData#state{item_ids = []},
    do_read_order_lines(1, NewStateData).

do_read_order_lines(
  I,
  #state{order_lines = OrderLines} = StateData
) when I > length(OrderLines) ->
    {next_state, read_stocks, StateData, 0};
do_read_order_lines(
  I,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_lines = OrderLines,
         item_ids = ItemIds} = StateData
) ->
    {OrderId, OrderLineNumber} = lists:nth(I, OrderLines),

    Key = term_to_binary({WarehouseId, DistrictId, OrderId, OrderLineNumber}),
    case riak_kv_transactional_client:get(WarehouseNode, ?ORDER_LINES_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, OrderLineObject} ->
            OrderLine = binary_to_term(riak_object:get_value(OrderLineObject)),

            NewStateData = StateData#state{item_ids = [OrderLine#order_line.ol_i_id | ItemIds]},
            do_read_order_lines(I + 1, NewStateData)
    end.

read_stocks(timeout, #state{item_ids = ItemIds} = StateData) ->
    NewStateData = StateData#state{item_ids = lists:usort(ItemIds)},
    do_read_stocks(1, NewStateData).

do_read_stocks(
  I,
  #state{item_ids = ItemIds} = StateData
) when I > length(ItemIds) ->
    {next_state, commit_transaction, StateData, 0};
do_read_stocks(
  I,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         item_ids = ItemIds} = StateData
) ->
    ItemId = lists:nth(I, ItemIds),

    Key = term_to_binary({WarehouseId, ItemId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?STOCKS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, _StockObject} ->
            %Stock = binary_to_term(riak_object:get_value(StockObject)),

            do_read_stocks(I + 1, StateData)
    end.

commit_transaction(timeout, #state{client = Client, requester = Requester} = StateData) ->
    Result = riak_kv_transactional_client:commit_transaction(Client),
    erlang:send(Requester, Result),
    {stop, normal, StateData}.

abort(timeout, #state{requester = Requester} = StateData) ->
    erlang:send(Requester, {error, aborted}),
    {stop, normal, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

terminate(Reason, _StateName, _State) ->
    Reason.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
