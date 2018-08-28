-module(tpc_c_new_order_transaction_fsm).

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
         read_warehouse/2,
         update_district/2,
         read_customer/2,
         write_new_order/2,
         write_order_lines/2,
         write_order/2,
         commit_transaction/2,
         rollback/2,
         abort/2]).

-record(state, {worker_id,
                riak_nodes,
                client,
                requester,
                warehouse_id,
                warehouse_node,
                district_id,
                order_id,
                customer_id,
                count_items,
                all_local}).

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
    {next_state, read_warehouse, StateData, 0}.

read_warehouse(
  timeout,
  #state{worker_id = WorkerId,
         riak_nodes = RiakNodes,
         client = Client} = StateData
) ->
    RiakNodesLength = length(RiakNodes),

    WarehouseId = ((WorkerId - 1) rem RiakNodesLength) + 1,
    WarehouseNode = lists:nth(((WarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),

    Key = term_to_binary({WarehouseId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?WAREHOUSES_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, _WarehouseObject} ->
            %Warehouse = binary_to_term(riak_object:get_value(WarehouseObject)),

            NewStateData = StateData#state{warehouse_id = WarehouseId,
                                           warehouse_node = WarehouseNode},
            {next_state, update_district, NewStateData, 0}
    end.

update_district(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode} = StateData
) ->
    DistrictId = tpc_c_helpers:uniform_random_int(1, ?N_DISTRICTS_PER_WAREHOUSE),

    Key = term_to_binary({WarehouseId, DistrictId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?DISTRICTS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, DistrictObject} ->
            District = binary_to_term(riak_object:get_value(DistrictObject)),

            NewDistrict = District#district{d_next_o_id = District#district.d_next_o_id + 1},

            Value = term_to_binary(NewDistrict),
            ok = riak_kv_transactional_client:put(WarehouseNode, ?DISTRICTS_BUCKET, Key, Value, Client),

            NewStateData = StateData#state{district_id = DistrictId,
                                           order_id = District#district.d_next_o_id},
            {next_state, read_customer, NewStateData, 0}
    end.

read_customer(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId} = StateData
) ->
    CustomerId = tpc_c_helpers:non_uniform_random_int(0, 1023, 1, ?N_CUSTOMERS_PER_DISTRICT),

    Key = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?CUSTOMERS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, _CustomerObject} ->
            %Customer = binary_to_term(riak_object:get_value(CustomerObject)),

            NewStateData = StateData#state{customer_id = CustomerId},
            {next_state, write_new_order, NewStateData, 0}
    end.

write_new_order(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_id = OrderId} = StateData
) ->
    NewOrder = #new_order{no_o_id = OrderId,
                          no_d_id = DistrictId,
                          no_w_id = WarehouseId},

    Key = term_to_binary({WarehouseId, DistrictId, OrderId}),
    Value = term_to_binary(NewOrder),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?NEW_ORDERS_BUCKET, Key, Value, Client),

    {next_state, write_order_lines, StateData, 0}.

write_order_lines(timeout, StateData) ->
    CountItems = tpc_c_helpers:uniform_random_int(5, 15),
    NewStateData = StateData#state{count_items = CountItems,
                                   all_local = 1},
    do_write_order_lines(1, NewStateData).

do_write_order_lines(
  OrderLineNumber,
  #state{count_items = CountItems} = StateData
) when OrderLineNumber > CountItems ->
    {next_state, write_order, StateData, 0};
do_write_order_lines(
  OrderLineNumber,
  #state{riak_nodes = RiakNodes,
         client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_id = OrderId,
         count_items = CountItems,
         all_local = AllLocal} = StateData
) ->
    RiakNodesLength = length(RiakNodes),

    Rbk = tpc_c_helpers:uniform_random_int(1, 100),
    ItemId = if
                 (OrderLineNumber == CountItems) and (Rbk == 1) -> -1;
                 true -> tpc_c_helpers:non_uniform_random_int(0, 8191, 1, ?N_ITEMS)
             end,
    SupplyingWarehouseId = choose_supplying_warehouse(WarehouseId, RiakNodesLength),
    SupplyingWarehouseNode = lists:nth(((SupplyingWarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),
    Quantity = tpc_c_helpers:uniform_random_int(1, 10),

    ItemNode = lists:nth(((ItemId - 1) rem length(RiakNodes)) + 1, RiakNodes),
    ItemKey = term_to_binary({ItemId}),
    case riak_kv_transactional_client:get(ItemNode, ?ITEMS_BUCKET, ItemKey, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {error, not_found} ->
            {next_state, rollback, StateData, 0};
        {ok, ItemObject} ->
            Item = binary_to_term(riak_object:get_value(ItemObject)),

            StockKey = term_to_binary({SupplyingWarehouseId, ItemId}),
            case riak_kv_transactional_client:get(SupplyingWarehouseNode, ?STOCKS_BUCKET, StockKey, Client) of
                {error, aborted} ->
                    {next_state, abort, StateData, 0};
                {ok, StockObject} ->
                    Stock = binary_to_term(riak_object:get_value(StockObject)),

                    StockQuantity = Stock#stock.s_quantity,
                    NewStockQuantity = case (StockQuantity - Quantity) >= 10 of
                                           true -> StockQuantity - 10;
                                           false -> StockQuantity - Quantity + 91
                                       end,
                    NewStockRemoteCnt = case SupplyingWarehouseId of 
                                            WarehouseId -> Stock#stock.s_remote_cnt;
                                            _ -> Stock#stock.s_remote_cnt + 1
                                        end,
                    NewStock = Stock#stock{s_quantity = NewStockQuantity,
                                           s_ytd = Stock#stock.s_ytd + Quantity,
                                           s_order_cnt = Stock#stock.s_order_cnt + 1,
                                           s_remote_cnt = NewStockRemoteCnt},

                    Value1 = term_to_binary(NewStock),
                    ok = riak_kv_transactional_client:put(SupplyingWarehouseNode, ?STOCKS_BUCKET, StockKey, Value1, Client),

                    OrderLine = #order_line{ol_o_id = OrderId,
                                            ol_w_id = WarehouseId,
                                            ol_d_id = DistrictId, 
                                            ol_number = OrderLineNumber,
                                            ol_i_id = ItemId,
                                            ol_supply_w_id = SupplyingWarehouseId, 
                                            ol_delivery_d = calendar:now_to_universal_time(os:timestamp()),
                                            ol_quantity = Quantity,
                                            ol_amount = Quantity * Item#item.i_price,
                                            ol_dist_info = stock_district_info(DistrictId, Stock)},

                    OrderLineKey = term_to_binary({WarehouseId, DistrictId, OrderId, OrderLineNumber}),
                    Value2 = term_to_binary(OrderLine),
                    ok = riak_kv_transactional_client:put(WarehouseNode, ?ORDER_LINES_BUCKET, OrderLineKey, Value2, Client),

                    NewAllLocal = case SupplyingWarehouseId of
                                      WarehouseId -> AllLocal;
                                      _ -> 0 
                                  end,
                    NewStateData = StateData#state{all_local = NewAllLocal},
                    do_write_order_lines(OrderLineNumber + 1, NewStateData)
            end
    end.

write_order(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_id = OrderId,
         customer_id = CustomerId, 
         count_items = CountItems,
         all_local = AllLocal} = StateData
) ->
    Order = #order{o_id = OrderId,
                   o_d_id = DistrictId,
                   o_w_id = WarehouseId,
                   o_c_id = CustomerId,
                   o_carrier_id = null,
                   o_ol_cnt = CountItems,
                   o_all_local = AllLocal},

    Key1 = term_to_binary({WarehouseId, DistrictId, OrderId}),
    Value = term_to_binary(Order),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?ORDERS_BUCKET, Key1, Value, Client),

    Key2 = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?LAST_CUSTOMER_ORDERS, Key2, Value, Client),

    {next_state, commit_transaction, StateData, 0}.

commit_transaction(timeout, #state{client = Client, requester = Requester} = StateData) ->
    Result = riak_kv_transactional_client:commit_transaction(Client),
    erlang:send(Requester, Result),
    {stop, normal, StateData}.

rollback(timeout, #state{client = Client, requester = Requester} = StateData) ->
    Result = riak_kv_transactional_client:rollback_transaction(Client),
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

% Private functions
choose_supplying_warehouse(WarehouseId, NWarehouses) ->
    Random = tpc_c_helpers:uniform_random_int(1, 100),
    case Random of
        1 -> tpc_c_helpers:uniform_random_int(1, NWarehouses, WarehouseId);
        _ -> WarehouseId
    end.

stock_district_info(1 = _DistrictId, #stock{s_dist_01 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(2 = _DistrictId, #stock{s_dist_02 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(3 = _DistrictId, #stock{s_dist_03 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(4 = _DistrictId, #stock{s_dist_04 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(5 = _DistrictId, #stock{s_dist_05 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(6 = _DistrictId, #stock{s_dist_06 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(7 = _DistrictId, #stock{s_dist_07 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(8 = _DistrictId, #stock{s_dist_08 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(9 = _DistrictId, #stock{s_dist_09 = StockDistrictInfo}) -> StockDistrictInfo;
stock_district_info(10 = _DistrictId, #stock{s_dist_10 = StockDistrictInfo}) -> StockDistrictInfo.
