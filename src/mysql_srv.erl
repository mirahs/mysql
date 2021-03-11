-module(mysql_srv).

-behaviour(gen_server).

-export([
    start_link/0

    ,add_pool/2
    ,fetch/4

    ,callback_start/2
    ,callback_result/2
]).

-export([
    init/1
    ,handle_call/3
    ,handle_cast/2
    ,handle_info/2
    ,terminate/2
    ,code_change/3
]).

-include("mysql.hrl").

-record(state, {}).

-define(ticket_heart, erlang:send_after(timer:seconds(?mysql_heart_time), erlang:self(), heart)).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


add_pool(PoolId, MysqlSide) ->
    gen_server:call(?MODULE, {add_pool, PoolId, MysqlSide}).

fetch(PoolId, Pid, Ref, Query) ->
    gen_server:cast(?MODULE, {fetch, PoolId, Pid, Ref, Query}).

callback_start(PoolId, SrvName) ->
    gen_server:cast(?MODULE, {callback_start, PoolId, SrvName}).

callback_result(PoolId, Ref) ->
    gen_server:cast(?MODULE, {callback_result, PoolId, Ref}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    erlang:process_flag(trap_exit, true),
    ?ticket_heart,
    {ok, #state{}}.

handle_call({add_pool, PoolId, #mysql_side{num = Num} = MysqlSide}, _From, State) ->
    case pool_get(PoolId) of
        undefined ->
            Cores	= mysql_util:core_count(),
            Num2	= ?IF(Num >= Cores, Num, Cores),
            NumFinal= ?IF(Num2 >= ?mysql_pool_conn_num, Num2, ?mysql_pool_conn_num),
            ChildSpecs = child_specs(NumFinal, Cores, PoolId, MysqlSide, []),
            mysql_side_sup:start_child(ChildSpecs),
            MysqlPool = #mysql_pool{pool_id = PoolId},
            pool_put(PoolId, MysqlPool),

            Pools	= pools_get(),
            Pools2	= [PoolId | lists:delete(PoolId, Pools)],
            pools_put(Pools2);
        _ -> ?ERR("Already add_pool:~p,MysqlSide:~p", [PoolId, MysqlSide]), skip
    end,
    {reply, ok, State};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({fetch, PoolId, Pid, Ref, Query}, State) ->
    case pool_get(PoolId) of
        Pool = #mysql_pool{} ->
            Pool2 = fetch_handle(Pool, Pid, Ref, Query),
            pool_put(PoolId, Pool2);
        _ -> Pid ! {error, Ref, lists:concat(["PoolId (", PoolId, ") not exist!"])}
    end,
    {noreply, State};

handle_cast({callback_start, PoolId, SrvName}, State) ->
    case pool_get(PoolId) of
        #mysql_pool{pool_ready = PoolReady, pool_working = PoolWorking} = Pool ->
            PoolReady2	= [SrvName | lists:delete(SrvName, PoolReady)],
            PoolWorking2= lists:keydelete(SrvName, 4, PoolWorking),
            Pool2 		= Pool#mysql_pool{pool_ready = PoolReady2, pool_working = PoolWorking2},
            pool_put(PoolId, Pool2);
        _ -> skip
    end,
    {noreply, State};

handle_cast({callback_result, PoolId, Ref}, State) ->
    case pool_get(PoolId) of
        #mysql_pool{} = Pool ->
            Pool2 = callback_result_handle(Pool, Ref),
            Pool3 = queue_handle(Pool2#mysql_pool.queue, Pool2#mysql_pool{queue = []}),
            pool_put(PoolId, Pool3);
        _ -> ?ERR("callback_result but PoolId(~p) not exist"), skip
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(heart, State) ->
    ?ticket_heart,
    heart_handle(),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State)  ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% 发送查询
fetch_handle(Pool, FromPid, Ref, Query)->
    case Pool#mysql_pool.pool_ready of
        [] ->
            Queue = Pool#mysql_pool.queue,
            Pool#mysql_pool{queue = Queue ++ [{FromPid,Ref,Query}]};
        [SideSrvName | Readys] ->
            Time = mysql_util:seconds(),
            mysql_side_srv:fetch_cast(SideSrvName, {FromPid, Ref}, Query),
            Work = {Ref, FromPid, Time, SideSrvName},
            Pool#mysql_pool{pool_ready = Readys, pool_working = [Work | Pool#mysql_pool.pool_working]}
    end.

%% 接收结果
callback_result_handle(Pool, Ref)->
    case lists:keytake(Ref, 1, Pool#mysql_pool.pool_working) of
        {value, {Ref, _FromPid, _Time, SideSrvName}, Working} ->
            Readys = Pool#mysql_pool.pool_ready,
            Pool#mysql_pool{pool_ready = Readys ++ [SideSrvName], pool_working = Working};
        false -> Pool
    end.

%% 接收结果时看看队列是否有存货有就发出去
queue_handle([], Pool) -> Pool;
queue_handle([{FromPid,Ref,Query} | Queue], Pool)->
    case Pool#mysql_pool.pool_ready of
        [] -> Pool#mysql_pool{queue = Queue ++ [{FromPid, Ref, Query}]};
        _ ->
            Pool2 = fetch_handle(Pool, FromPid, Ref, Query),
            queue_handle(Queue, Pool2)
    end.

heart_handle() ->
    Pools = pools_get(),
    Fun = fun
              (PoolId) ->
                  case pool_get(PoolId) of
                      #mysql_pool{pool_ready = Readys} -> [mysql_side_srv:heart_cast(SideSrvName) || SideSrvName <- Readys];
                      _ -> skip
                  end
          end,
    lists:foreach(Fun, Pools).


child_specs(0, _Cores, _PoolId, _MysqlSide, ChildSpecs) ->
    ChildSpecs;
child_specs(N, Cores, PoolId, MysqlSide, ChildSpecs) ->
    SrvName		= mysql_util:to_atom(lists:concat([mysql_side_srv_, PoolId, "_" , N])),
    ChildSpec 	= {SrvName, {mysql_side_srv, start_link, [N, Cores, SrvName, MysqlSide#mysql_side{pool_id = PoolId, srv_name = SrvName}]}, permanent, brutal_kill, worker, [mysql_side_srv]},
    child_specs(N - 1, Cores, PoolId, MysqlSide, [ChildSpec | ChildSpecs]).


pools_get() ->
    case erlang:get(pools) of
        undefined -> [];
        Pools -> Pools
    end.
pools_put(Pools) ->
    erlang:put(pools, Pools).
pool_get(PoolId) ->
    erlang:get(PoolId).
pool_put(PoolId, Pool) ->
    erlang:put(PoolId, Pool).
