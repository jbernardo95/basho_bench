-module(tpc_c_payment_transaction_fsm).

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
         update_warehouse/2,
         update_district/2,
         read_customer/2,
         write_customer/2,
         write_history/2,
         commit_transaction/2,
         rollback/2,
         abort/2]).

-record(state, {worker_id,
                riak_nodes,
                client,
                requester,
                warehouse_id,
                warehouse_node,
                warehouse_name,
                amount,
                district_id,
                district_name,
                customer,
                customer_node}).

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
    {next_state, update_warehouse, StateData, 0}.

update_warehouse(
  timeout,
  #state{worker_id = WorkerId,
         riak_nodes = RiakNodes,
         client = Client} = StateData
) ->
    RiakNodesLength = length(RiakNodes),

    WarehouseId = ((WorkerId - 1) rem RiakNodesLength) + 1,
    WarehouseNode = lists:nth(((WarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),
    Amount = tpc_c_helpers:uniform_random_float(1, 5000, 2),

    Key = term_to_binary({WarehouseId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?WAREHOUSES_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, WarehouseObject} ->
            Warehouse = binary_to_term(riak_object:get_value(WarehouseObject)),

            NewWarehouse = Warehouse#warehouse{w_ytd = Warehouse#warehouse.w_ytd + Amount},

            Value = term_to_binary(NewWarehouse),
            ok = riak_kv_transactional_client:put(WarehouseNode, ?WAREHOUSES_BUCKET, Key, Value, Client),

            NewStateData = StateData#state{warehouse_id = WarehouseId,
                                           warehouse_node = WarehouseNode,
                                           warehouse_name = Warehouse#warehouse.w_name,
                                           amount = Amount},
            {next_state, update_district, NewStateData, 0}
    end.

update_district(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         amount = Amount} = StateData
) ->
    DistrictId = tpc_c_helpers:uniform_random_int(1, ?N_DISTRICTS_PER_WAREHOUSE),

    Key = term_to_binary({WarehouseId, DistrictId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?DISTRICTS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, DistrictObject} ->
            District = binary_to_term(riak_object:get_value(DistrictObject)),

            NewDistrict = District#district{d_ytd = District#district.d_ytd + Amount},

            Value = term_to_binary(NewDistrict),
            ok = riak_kv_transactional_client:put(WarehouseNode, ?DISTRICTS_BUCKET, Key, Value, Client),

            NewStateData = StateData#state{district_id = DistrictId,
                                           district_name = District#district.d_name},

            {next_state, read_customer, NewStateData, 0}
    end.

read_customer(
  timeout,
  #state{riak_nodes = RiakNodes,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId} = StateData
) ->
    RandomX = tpc_c_helpers:uniform_random_int(1, 100),
    if
        RandomX =< 85 ->
            CustomerWarehouseId = WarehouseId,
            CustomerNode = WarehouseNode,
            CustomerDistrictId = DistrictId;
        true ->
            RiakNodesLength = length(RiakNodes),
            CustomerWarehouseId = tpc_c_helpers:uniform_random_int(1, RiakNodesLength, WarehouseId),
            CustomerNode = lists:nth(((CustomerWarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),
            CustomerDistrictId = tpc_c_helpers:uniform_random_int(1, ?N_DISTRICTS_PER_WAREHOUSE)
    end,

    RandomY = tpc_c_helpers:uniform_random_int(1, 100),
    if
        RandomY =< 60 ->
            read_customer_by_last(CustomerNode, CustomerWarehouseId, CustomerDistrictId, StateData);
        true ->
            read_customer_by_id(CustomerNode, CustomerWarehouseId, CustomerDistrictId, StateData)
    end.

read_customer_by_last(Node, WarehouseId, DistrictId, #state{client = Client} = StateData) ->
    Random = tpc_c_helpers:non_uniform_random_int(0, 255, 0, 999),
    CustomerLast = tpc_c_helpers:generate_customer_last_name(Random),

    Key = term_to_binary({WarehouseId, DistrictId, CustomerLast}),
    case riak_kv_transactional_client:get(Node, ?CUSTOMERS_LAST_NAME_INDEX, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {error, not_found} ->
            {next_state, rollback, StateData, 0};
        {ok, CustomerIdsObject} ->
            CustomerIds = riak_object:get_value(CustomerIdsObject),
            Customers = lists:map(fun(CustomerId) ->
                                          Key = term_to_binary({WarehouseId, DistrictId, CustomerId}),
                                          {ok, CustomerObject} = riak_kv_transactional_client:get(Node, ?CUSTOMERS_BUCKET, Key, Client),
                                          Customer = binary_to_term(riak_object:get_value(CustomerObject)),
                                          {Customer#customer.c_first, Customer}
                                  end, CustomerIds),
            SortedCustomers = lists:sort(Customers),
            Middle = round(length(SortedCustomers) / 2),
            {_, Customer} = lists:nth(Middle, SortedCustomers),

            NewStateData = StateData#state{customer = Customer, customer_node = Node},
            {next_state, write_customer, NewStateData, 0}
    end.

read_customer_by_id(Node, WarehouseId, DistrictId, #state{client = Client} = StateData) ->
    CustomerId = tpc_c_helpers:non_uniform_random_int(0, 1023, 1, ?N_CUSTOMERS_PER_DISTRICT),

    Key = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    case riak_kv_transactional_client:get(Node, ?CUSTOMERS_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, CustomerObject} ->
            Customer = binary_to_term(riak_object:get_value(CustomerObject)),

            NewStateData = StateData#state{customer = Customer, customer_node = Node},
            {next_state, write_customer, NewStateData, 0}
    end.

write_customer(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         district_id = DistrictId,
         amount = Amount,
         customer = Customer,
         customer_node = CustomerNode} = StateData
) ->
    NewCustomer = Customer#customer{c_balance = Customer#customer.c_balance - Amount,
                                    c_ytd_payment = Customer#customer.c_ytd_payment + Amount,
                                    c_payment_cnt = Customer#customer.c_payment_cnt + 1,
                                    c_data = case Customer#customer.c_credit of
                                                 "BC" ->
                                                     lists:sublist(integer_to_list(Customer#customer.c_id) ++
                                                                   integer_to_list(Customer#customer.c_d_id) ++
                                                                   integer_to_list(Customer#customer.c_w_id) ++
                                                                   integer_to_list(DistrictId) ++
                                                                   integer_to_list(WarehouseId) ++
                                                                   integer_to_list(Amount) ++
                                                                   Customer#customer.c_data, 500);
                                                 _ ->
                                                     Customer#customer.c_data
                                             end},

    Key = term_to_binary({Customer#customer.c_w_id, Customer#customer.c_d_id, Customer#customer.c_id}),
    Value = term_to_binary(NewCustomer),
    ok = riak_kv_transactional_client:put(CustomerNode, ?CUSTOMERS_BUCKET, Key, Value, Client),

    {next_state, write_history, StateData, 0}.

write_history(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         warehouse_name = WarehouseName,
         amount = Amount,
         district_id = DistrictId,
         district_name = DistrictName,
         customer = Customer} = StateData
) ->
    HistoryDate = calendar:now_to_universal_time(os:timestamp()),
    History = #history{h_c_id = Customer#customer.c_id,
                       h_c_d_id = Customer#customer.c_d_id,
                       h_c_w_id = Customer#customer.c_w_id,
                       h_d_id = DistrictId,
                       h_w_id = WarehouseId,
                       h_date = HistoryDate,
                       h_amount = Amount,
                       h_data = WarehouseName ++ "    " ++ DistrictName},

    Key = term_to_binary({WarehouseId, DistrictId, Customer#customer.c_id, HistoryDate}),
    Value = term_to_binary(History),
    ok = riak_kv_transactional_client:put(WarehouseNode, ?HISTORIES_BUCKET, Key, Value, Client),

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
