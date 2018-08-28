-module(tpc_c_order_status_transaction_fsm).

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
         read_customer/2,
         read_order/2,
         read_order_lines/2,
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
                customer_id,
                order_id,
                order_count}).

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
    {next_state, read_customer, StateData, 0}.

read_customer(
  timeout,
  #state{worker_id = WorkerId,
         riak_nodes = RiakNodes} = StateData
) ->
    RiakNodesLength = length(RiakNodes),

    WarehouseId = ((WorkerId - 1) rem RiakNodesLength) + 1,
    WarehouseNode = lists:nth(((WarehouseId - 1) rem RiakNodesLength) + 1, RiakNodes),
    DistrictId = tpc_c_helpers:uniform_random_int(1, ?N_DISTRICTS_PER_WAREHOUSE),

    NewStateData = StateData#state{warehouse_id = WarehouseId,
                                   warehouse_node = WarehouseNode,
                                   district_id = DistrictId},

    RandomY = tpc_c_helpers:uniform_random_int(1, 100),
    if
        RandomY =< 60 ->
            read_customer_by_last(NewStateData);
        true ->
            read_customer_by_id(NewStateData)
    end.

read_customer_by_last(
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId} = StateData
) ->
    Random = tpc_c_helpers:non_uniform_random_int(0, 255, 0, 999),
    CustomerLast = tpc_c_helpers:generate_customer_last_name(Random),

    Key = term_to_binary({WarehouseId, DistrictId, CustomerLast}),
    case riak_kv_transactional_client:get(WarehouseNode, ?CUSTOMERS_LAST_NAME_INDEX, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {error, not_found} ->
            {next_state, rollback, StateData, 0};
        {ok, CustomerIdsObject} ->
            CustomerIds = riak_object:get_value(CustomerIdsObject),
            Customers = lists:map(fun(CustomerId) ->
                                          Key = term_to_binary({WarehouseId, DistrictId, CustomerId}),
                                          {ok, CustomerObject} = riak_kv_transactional_client:get(WarehouseNode, ?CUSTOMERS_BUCKET, Key, Client),
                                          Customer = binary_to_term(riak_object:get_value(CustomerObject)),
                                          {Customer#customer.c_first, Customer}
                                  end, CustomerIds),
            SortedCustomers = lists:sort(Customers),
            Middle = round(length(SortedCustomers) / 2),
            {_, Customer} = lists:nth(Middle, SortedCustomers),

            NewStateData = StateData#state{customer_id = Customer#customer.c_id},
            {next_state, read_order, NewStateData, 0}
    end.

read_customer_by_id(
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
            {next_state, read_order, NewStateData, 0}
    end.

read_order(
  timeout,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         customer_id = CustomerId} = StateData
) ->
    Key = term_to_binary({WarehouseId, DistrictId, CustomerId}),
    case riak_kv_transactional_client:get(WarehouseNode, ?LAST_CUSTOMER_ORDERS, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, OrderObject} ->
            Order = binary_to_term(riak_object:get_value(OrderObject)),

            NewStateData = StateData#state{order_id = Order#order.o_id,
                                           order_count = Order#order.o_ol_cnt},
            {next_state, read_order_lines, NewStateData, 0}
    end.

read_order_lines(timeout, StateData) ->
    do_read_order_lines(1, StateData).

do_read_order_lines(
  OrderLineNumber,
  #state{order_count = OrderCount} = StateData
) when OrderLineNumber > OrderCount ->
    {next_state, commit_transaction, StateData, 0};
do_read_order_lines(
  OrderLineNumber,
  #state{client = Client,
         warehouse_id = WarehouseId,
         warehouse_node = WarehouseNode,
         district_id = DistrictId,
         order_id = OrderId} = StateData
) ->
    Key = term_to_binary({WarehouseId, DistrictId, OrderId, OrderLineNumber}),
    case riak_kv_transactional_client:get(WarehouseNode, ?ORDER_LINES_BUCKET, Key, Client) of
        {error, aborted} ->
            {next_state, abort, StateData, 0};
        {ok, _OrderLineObject} ->
            %OrderLine = binary_to_term(riak_object:get_value(OrderLineObject)),

            do_read_order_lines(OrderLineNumber + 1, StateData)
    end.

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
