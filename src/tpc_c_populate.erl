-module(tpc_c_populate).

-include("tpc_c.hrl").

-define(AMOUNT, 10.0).

-export([populate/3]).

%% ===================================================================
%% Public API
%% ===================================================================

populate(WarehouseId, RiakNodes, Client) ->
    WarehouseNode = lists:nth(((WarehouseId - 1) rem length(RiakNodes)) + 1, RiakNodes),

    case WarehouseId of
        1 -> populate_items(RiakNodes, Client);
        _ -> ok
    end,

    populate_warehouse(WarehouseId, WarehouseNode, Client),

    populate_stocks(WarehouseId, WarehouseNode, Client),

    PopulateFun = fun(DistrictId) ->
                          populate_district(DistrictId, WarehouseId, WarehouseNode, Client),

                          CustomerIds = lists:seq(1, ?N_CUSTOMERS_PER_DISTRICT),
                          lists:foreach(fun(CustomerId) ->
                                                populate_customer(CustomerId, DistrictId, WarehouseId, WarehouseNode, Client),
                                                populate_history(CustomerId, DistrictId, WarehouseId, WarehouseNode, Client)
                                        end,CustomerIds),

                          ShuffledCustomerIds = [CI1 || {_, CI1} <- lists:sort([{random:uniform(), CI2} || CI2 <- CustomerIds])],
                          lists:foreach(fun(OrderId) ->
                                                OrderDate = calendar:now_to_universal_time(os:timestamp()),
                                                OrderCount = tpc_c_helpers:uniform_random_int(5, 15),
                                                CustomerId = lists:nth(OrderId, ShuffledCustomerIds),
                                                populate_order(OrderId, OrderDate, OrderCount, CustomerId, DistrictId, WarehouseId, WarehouseNode, Client),
                                                populate_order_lines(OrderId, OrderDate, OrderCount, DistrictId, WarehouseId, WarehouseNode, Client),
                                                populate_new_order(OrderId, DistrictId, WarehouseId, WarehouseNode, Client)
                                        end, lists:seq(1, ?N_ORDERS_PER_DISTRICT))
                  end,
    lists:foreach(PopulateFun, lists:seq(1, ?N_DISTRICTS_PER_WAREHOUSE)),

    case WarehouseId of
        1 ->
            % Hack to init the client whith a snapshot equal to the latest one
            LatestSnapshot = term_to_binary(latest_snapshot),
            ok = riak_kv_transactional_client:put(WarehouseNode, LatestSnapshot, LatestSnapshot, LatestSnapshot, Client);
        _ ->
            ok
    end.

populate_items(RiakNodes, Client) ->
    PopulateFun = fun(ItemId) ->
                          Item = #item{i_id = ItemId,
                                       i_im_id = tpc_c_helpers:uniform_random_int(1, 10000), 
                                       i_name = tpc_c_helpers:random_a_string(14, 24),
                                       i_price = tpc_c_helpers:uniform_random_float(1, 100, 2),
                                       i_data = tpc_c_helpers:random_data()},

                          Node = lists:nth(((ItemId - 1) rem length(RiakNodes)) + 1, RiakNodes),
                          Key = term_to_binary({ItemId}),
                          Value = term_to_binary(Item),
                          ok = riak_kv_transactional_client:put(Node, ?ITEMS_BUCKET, Key, Value, Client)
                  end,
    lists:foreach(PopulateFun, lists:seq(1, ?N_ITEMS)).

populate_warehouse(WarehouseId, WarehouseNode, Client) ->
    Warehouse = #warehouse{w_id = WarehouseId,
                           w_name = tpc_c_helpers:random_a_string(6, 10),
                           w_street1 = tpc_c_helpers:random_a_string(10, 20), 
                           w_street2 = tpc_c_helpers:random_a_string(10, 20), 
                           w_city = tpc_c_helpers:random_a_string(10, 20), 
                           w_state = tpc_c_helpers:random_a_string(2), 
                           w_zip = tpc_c_helpers:random_a_string(4) ++ "11111",
                           w_tax = tpc_c_helpers:uniform_random_float(0.0, 0.2, 4),
                           w_ytd = ?N_DISTRICTS_PER_WAREHOUSE * ?N_ORDERS_PER_DISTRICT * ?AMOUNT},

    Key = term_to_binary({WarehouseId}),
    Value = term_to_binary(Warehouse),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?WAREHOUSES_BUCKET, Key, Value, Client).

populate_stocks(WarehouseId, WarehouseNode, Client) ->
    PopulateFun = fun(ItemId) ->
                          Stock = #stock{s_i_id = ItemId,
                                         s_w_id = WarehouseId,
                                         s_quantity = tpc_c_helpers:uniform_random_int(10, 100),
                                         s_dist_01 = tpc_c_helpers:random_a_string(24),
                                         s_dist_02 = tpc_c_helpers:random_a_string(24),
                                         s_dist_03 = tpc_c_helpers:random_a_string(24),
                                         s_dist_04 = tpc_c_helpers:random_a_string(24),
                                         s_dist_05 = tpc_c_helpers:random_a_string(24),
                                         s_dist_06 = tpc_c_helpers:random_a_string(24),
                                         s_dist_07 = tpc_c_helpers:random_a_string(24),
                                         s_dist_08 = tpc_c_helpers:random_a_string(24),
                                         s_dist_09 = tpc_c_helpers:random_a_string(24),
                                         s_dist_10 = tpc_c_helpers:random_a_string(24),
                                         s_ytd = 0,
                                         s_order_cnt = 0,
                                         s_remote_cnt = 0,
                                         s_data = tpc_c_helpers:random_data()},

                          Key = term_to_binary({WarehouseId, ItemId}),
                          Value = term_to_binary(Stock),
                          ok = riak_kv_transactional_client:put(WarehouseNode, ?STOCKS_BUCKET, Key, Value, Client)
                  end,
    lists:foreach(PopulateFun, lists:seq(1, ?N_ITEMS)).

populate_district(DistrictId, WarehouseId, WarehouseNode, Client) ->
    District = #district{d_id = DistrictId,
                         d_w_id = WarehouseId, 
                         d_name = tpc_c_helpers:random_a_string(6, 10),
                         d_street1 = tpc_c_helpers:random_a_string(10, 20),
                         d_street2 = tpc_c_helpers:random_a_string(10, 20),
                         d_city = tpc_c_helpers:random_a_string(10, 20),
                         d_state = tpc_c_helpers:random_a_string(2),
                         d_zip = tpc_c_helpers:random_a_string(4) ++ "11111",
                         d_tax = tpc_c_helpers:uniform_random_float(0.0, 0.2, 4),
                         d_ytd = ?N_ORDERS_PER_DISTRICT * ?AMOUNT,
                         d_next_o_id = ?N_ORDERS_PER_DISTRICT + 1},

    Key = term_to_binary({WarehouseId, DistrictId}),
    Value = term_to_binary(District),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?DISTRICTS_BUCKET, Key, Value, Client).

populate_customer(CustomerId, DistrictId, WarehouseId, WarehouseNode, Client) ->
    CustomerLast = if
                       CustomerId =< 1000 ->
                           tpc_c_helpers:generate_customer_last_name(CustomerId - 1);
                       true ->
                           Random = tpc_c_helpers:non_uniform_random_int(66, 255, 0, 999),
                           tpc_c_helpers:generate_customer_last_name(Random)
                   end,
    Customer = #customer{c_id = CustomerId,
                         c_d_id = DistrictId,
                         c_w_id = WarehouseId,
                         c_first = tpc_c_helpers:random_a_string(8, 16),
                         c_middle = "OE",
                         c_last = CustomerLast,
                         c_street1 = tpc_c_helpers:random_a_string(10, 20),
                         c_street2 = tpc_c_helpers:random_a_string(10, 20),
                         c_city = tpc_c_helpers:random_a_string(10, 20),
                         c_state = tpc_c_helpers:random_a_string(2),
                         c_zip = tpc_c_helpers:random_a_string(4) ++ "11111",
                         c_phone = tpc_c_helpers:random_numeric_string(16),
                         c_since = calendar:now_to_universal_time(os:timestamp()),
                         c_credit = case tpc_c_helpers:uniform_random_int(1, 100) of
                                        10 -> "BC";
                                        _ -> "GC"
                                    end,
                         c_credit_lim = 50000.0,
                         c_discount = tpc_c_helpers:uniform_random_float(0.0, 0.5, 4),
                         c_balance = -?AMOUNT,
                         c_ytd_payment = ?AMOUNT,
                         c_payment_cnt = 1,
                         c_delivery_cnt = 0,
                         c_data = tpc_c_helpers:random_a_string(300, 500)},

    Key1 = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    Value1 = term_to_binary(Customer),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?CUSTOMERS_BUCKET, Key1, Value1, Client),

    Key2 = term_to_binary({WarehouseId, DistrictId, CustomerLast}),
    Value2 = case riak_kv_transactional_client:get(WarehouseNode, ?CUSTOMERS_LAST_NAME_INDEX, Key2, Client) of
                 {error, not_found} ->
                     [CustomerId];
                 {ok, CustomerIdsObject} ->
                     CustomerIds = riak_object:get_value(CustomerIdsObject),
                     [CustomerId | CustomerIds]
             end,
    ok = riak_kv_transactional_client:put(WarehouseNode, ?CUSTOMERS_LAST_NAME_INDEX, Key2, Value2, Client).

populate_history(CustomerId, DistrictId, WarehouseId, WarehouseNode, Client) ->
    HistoryDate = calendar:now_to_universal_time(os:timestamp()),
    History = #history{h_c_id = CustomerId,
                       h_c_d_id = DistrictId,
                       h_c_w_id = WarehouseId,
                       h_d_id = DistrictId,
                       h_w_id = WarehouseId,
                       h_date = HistoryDate,
                       h_amount = ?AMOUNT,
                       h_data = tpc_c_helpers:random_a_string(12, 24)}, 

    Key = term_to_binary({WarehouseId, DistrictId, CustomerId, HistoryDate}),
    Value = term_to_binary(History),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?HISTORIES_BUCKET, Key, Value, Client).

populate_order(OrderId, OrderDate, OrderCount, CustomerId, DistrictId, WarehouseId, WarehouseNode, Client) ->
    Order = #order{o_id = OrderId,
                   o_d_id = DistrictId,
                   o_w_id = WarehouseId,
                   o_c_id = CustomerId,
                   o_entry_d = OrderDate,
                   o_carrier_id = if
                                      OrderId < 2101 -> tpc_c_helpers:uniform_random_int(1, 10);
                                      true -> null
                                  end, 
                   o_ol_cnt = OrderCount,
                   o_all_local = 1},

    Key1 = term_to_binary({WarehouseId, DistrictId, OrderId}),
    Value = term_to_binary(Order),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?ORDERS_BUCKET, Key1, Value, Client),

    Key2 = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?LAST_CUSTOMER_ORDERS, Key2, Value, Client).

populate_order_lines(OrderId, OrderDate, OrderCount, DistrictId, WarehouseId, WarehouseNode, Client) ->
    lists:foreach(fun(OrderLineNumber) ->
                          populate_order_line(OrderLineNumber, OrderId, OrderDate, DistrictId, WarehouseId, WarehouseNode, Client)
                  end, lists:seq(1, OrderCount)).

populate_order_line(OrderLineNumber, OrderId, OrderDate, DistrictId, WarehouseId, WarehouseNode, Client) ->
    OrderLine = #order_line{ol_o_id = OrderId,
                            ol_d_id = DistrictId,
                            ol_w_id = WarehouseId,
                            ol_number = OrderLineNumber,
                            ol_i_id = tpc_c_helpers:uniform_random_int(1, ?N_ITEMS),
                            ol_supply_w_id = WarehouseId,
                            ol_delivery_d = if
                                                OrderId < 2101 -> OrderDate;
                                                true -> null
                                            end,
                            ol_quantity = 5,
                            ol_amount = if
                                            OrderId < 2101 -> 0.0;
                                            true -> tpc_c_helpers:uniform_random_float(0.01, 9999.99, 2)
                                        end,
                            ol_dist_info = tpc_c_helpers:random_a_string(24)},

    Key = term_to_binary({WarehouseId, DistrictId, OrderId, OrderLineNumber}),
    Value = term_to_binary(OrderLine),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?ORDER_LINES_BUCKET, Key, Value, Client).

populate_new_order(OrderId, _DistrictId, _WarehouseId, _WarehouseNode, _Client) when OrderId < 2101 -> ok;
populate_new_order(OrderId, DistrictId, WarehouseId, WarehouseNode, Client) ->
    NewOrder = #new_order{no_o_id = OrderId,
                          no_d_id = DistrictId,
                          no_w_id = WarehouseId},

    Key = term_to_binary({WarehouseId, DistrictId, OrderId}),
    Value = term_to_binary(NewOrder),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?NEW_ORDERS_BUCKET, Key, Value, Client).
