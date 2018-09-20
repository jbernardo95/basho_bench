-module(basho_bench_riak_kv_transactions_executor).

-export([execute_transaction/4]).

execute_transaction(TransactionType, NodeBucketKeyGen, ValueGen, Client) ->
    Start = os:timestamp(),
    Result = do_execute_transaction(TransactionType, NodeBucketKeyGen, ValueGen, Client),
    ElapsedUs = erlang:max(0, timer:now_diff(os:timestamp(), Start)),
    basho_bench_stats:op_complete(TransactionType, Result, ElapsedUs).

do_execute_transaction(TransactionType, NodeBucketKeyGen, ValueGen, Client) ->
    OperationsPerTransaction = basho_bench_config:get(operations_per_transaction),
    MidOperations = lists:foldl(fun({Operation, N}, Acc) ->
                                        [Operation || _I <- lists:seq(1, N)] ++ Acc
                                end, [], OperationsPerTransaction),
    Operations = [begin_transaction] ++ MidOperations ++ [commit_transaction],
    Nbkeys = NodeBucketKeyGen(TransactionType),

    execute_transaction_operations(Operations, Nbkeys, ValueGen, Client).

execute_transaction_operations([begin_transaction | RestOperations], Nbkeys, ValueGen, Client) ->
    ok = riak_kv_transactional_client:begin_transaction(Client),
    execute_transaction_operations(RestOperations, Nbkeys, ValueGen, Client);

execute_transaction_operations([get | RestOperations], [{Node, Bucket, Key} | RestNbkeys], ValueGen, Client) ->
    case riak_kv_transactional_client:get(Node, Bucket, Key, Client) of
        {error, aborted} -> {error, aborted};
        _ -> execute_transaction_operations(RestOperations, RestNbkeys, ValueGen, Client)
    end;

execute_transaction_operations([put | RestOperations], [{Node, Bucket, Key} | RestNbkeys], ValueGen, Client) ->
    ok = riak_kv_transactional_client:put(Node, Bucket, Key, ValueGen(), Client),
    execute_transaction_operations(RestOperations, RestNbkeys, ValueGen, Client);

execute_transaction_operations([update| RestOperations], [{Node, Bucket, Key} | RestNbkeys], ValueGen, Client) ->
    case riak_kv_transactional_client:get(Node, Bucket, Key, Client) of
        {error, aborted} -> {error, aborted};
        _ ->
            ok = riak_kv_transactional_client:put(Node, Bucket, Key, ValueGen(), Client),
            execute_transaction_operations(RestOperations, RestNbkeys, ValueGen, Client)
    end;

execute_transaction_operations([commit_transaction], [], _ValueGen, Client) ->
    riak_kv_transactional_client:commit_transaction(Client).
