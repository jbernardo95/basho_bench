%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_riakclient).

-export([new/1, run/4]).

-include("basho_bench.hrl").

-record(state, {client, populate_last_key}).

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

    MyNode = basho_bench_config:get(riakclient_mynode, [basho_bench, longnames]),
    case net_kernel:start(MyNode) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    Nodes = basho_bench_config:get(riakclient_nodes),
    Cookie = basho_bench_config:get(riakclient_cookie, 'riak'),
    establish_connection_to_nodes(Nodes, Cookie),

    global:sync(),

    TargetNode = lists:nth(((Id - 1) rem length(Nodes)) + 1, Nodes),
    case riak:client_connect(TargetNode) of
        {ok, Client} ->
            {ok, #state{client = Client, populate_last_key = undefined}};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to start riak_kv_transactional_client: ~p\n", [Reason2])
    end.

run(populate, NodeBucketKeyGen, ValueGen, #state{client = Client,
    populate_last_key = PopulateLastKey} = State) ->
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
run(leaf_tx_manager_transaction, NodeBucketKeyGen, ValueGen, #state{client = Client} = State) ->
    Robj = riak_object:new(<<"basho_bench_bucket">>, NodeBucketKeyGen(), ValueGen()),
    case Client:put(Robj, 1) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(root_tx_manager_transaction, NodeBucketKeyGen, ValueGen, #state{client = Client} = State) ->
    [{Node1, Bucket1, Key1},
     {Node2, Bucket2, Key2}] = NodeBucketKeyGen(root_tx_manager_transaction),

    riak_kv_transactional_client:begin_transaction(Client),

    riak_kv_transactional_client:put(Node1, Bucket1, Key1, ValueGen(), Client),

    riak_kv_transactional_client:put(Node2, Bucket2, Key2, ValueGen(), Client),

    case riak_kv_transactional_client:commit_transaction(Client) of
        ok ->
            {ok, State};
        {error, aborted} ->
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
