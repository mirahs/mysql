-module(mysql_side_srv).

-behaviour(gen_server).

-include("common.hrl").
-include("mysql.hrl").

-export([
	start_link/4

	,fetch_cast/3
	,heart_cast/1
]).

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).


start_link(N, Cores, SrvName, MysqlSide) ->
	gen_server:start_link({local, SrvName}, ?MODULE, [MysqlSide], [{spawn_opt, [{scheduler, N rem Cores + 1}]}]).


fetch_cast(SrvName, From, Query) ->
	gen_server:cast(SrvName, {fetch, From, Query}).

heart_cast(SrvName) ->
	gen_server:cast(SrvName, heart).


init([#mysql_side{host = Host, port = Port, pool_id = PoolId, srv_name = SrvName} = MysqlSide]) ->
	erlang:process_flag(trap_exit, true),
	case gen_tcp:connect(Host, Port, [binary, {packet, 0}]) of
		{ok, Socket} ->
			case mysql_auth:auth(MysqlSide#mysql_side{socket = Socket}) of
				{ok, MysqlSide2} ->
					Db 			= mysql_util:to_binary(MysqlSide#mysql_side.database),
					QueryUseDb 	= <<"USE ", Db/binary>>,
					case handle_cast({fetch, null, QueryUseDb}, MysqlSide2) of
						{noreply, MysqlSide3} ->
							case MysqlSide#mysql_side.charset of
								undefined ->
									mysql_srv:callback_start(PoolId, SrvName),
									{noreply, MysqlSide3};
								Charset ->
									EncodingBinary = mysql_util:to_binary(Charset),
									QueryEncoding  = <<"SET NAMES '", EncodingBinary/binary, "'">>,
									case handle_cast({fetch, null, QueryEncoding}, MysqlSide3) of
										{noreply, MysqlSide4} ->
											mysql_srv:callback_start(PoolId, SrvName),
											{ok, MysqlSide4};
										{stop, Reason, _State} ->
											{stop, {error, Reason}}
									end
							end;
						{stop, Reason, _State} ->
							{stop, {error, Reason}}
					end;
				Error -> {stop, Error}
			end;
		Error -> {stop, Error}
	end.

handle_call(_Msg, _From, State) ->
	{reply, ok, State}.

handle_cast({fetch, From, Query}, State) ->
	case mysql_mod:do_query(State#mysql_side.socket, State#mysql_side.bin, State#mysql_side.ver, Query) of
		{ok, Result, Bin2} ->
			case From of
				null -> skip;
				{null, Ref} -> mysql_srv:callback_result(State#mysql_side.pool_id, Ref);
				{Pid, Ref} ->
					Pid ! {ok, Ref, Result},
					mysql_srv:callback_result(State#mysql_side.pool_id, Ref)
			end,
			{noreply, State#mysql_side{bin = Bin2}};
		{error, Reason} ->
			?ERROR("Fetch Query:~p,Reason:~p", [Query, Reason]),
			{stop, Reason, State}
	end;

handle_cast(heart, State) ->
	Db		= mysql_util:to_binary(State#mysql_side.database),
	Query	= <<"USE ", Db/binary>>,
	handle_cast({fetch, null, Query}, State).

handle_info(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, State)  ->
	catch gen_tcp:close(State#mysql_side.socket),
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
