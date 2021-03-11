-module(mysql_logger).

-export([
	debug/1
	,debug/2
	,debug/4
	,info/1
	,info/2
	,info/4
	,error/1
	,error/2
	,error/4
]).


%%%===================================================================
%%% API
%%%===================================================================

%% 输出调试信息到控制台
debug(Msg) ->
	debug(Msg, []).
debug(Format, Args) ->
	debug(Format, Args, null, null).
debug(Format, Args, Mod, Line) ->
	Msg = format("mysql debug", Format, Args, Mod, Line),
	io:format("~ts", [Msg]).

%% 输出普通信息到控制台
info(Msg) ->
	info(Msg, []).
info(Format, Args) ->
	info(Format, Args, null, null).
info(Format, Args, Mod, Line) ->
	Msg = format("mysql info", Format, Args, Mod, Line),
	io:format("~ts", [Msg]).

%% 输出错误信息到控制台
error(Msg) ->
	?MODULE:error(Msg, []).
error(Format, Args) ->
	?MODULE:error(Format, Args, null, null).
error(Format, Args, Mod, Line) ->
	Msg = format("mysql error", Format, Args, Mod, Line),
	io:format("~ts", [Msg]).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @spec format(T, F, A, Mod, Line) -> list()
%% T = list()
%% F = list()
%% A = list()
%% Mod = list()
%% Line = int()
%% @doc 格式化日志信息
format(T, F, A, Mod, Line) ->
	{{Y, M, D}, {H, I, S}} = erlang:localtime(),
	Date = lists:concat([Y, "/", M, "/", D, "_", H, ":", I, ":", S]),
	case Line of
		null -> erlang:iolist_to_binary(io_lib:format(lists:concat(["## ", T, " ~s ", F, "~n"]), [Date] ++ A));
		_ -> erlang:iolist_to_binary(io_lib:format(lists:concat(["## ", T, " ~s [~w:~w] ", F, "~n"]), [Date, Mod, Line] ++ A))
	end.
