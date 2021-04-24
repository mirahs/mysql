%% -*- coding: latin-1 -*-
-module(mysql_logger_file).

-behaviour(gen_server).

-export([
    start_link/1

    ,info/1
    ,info/2
    ,info/4
    ,error/1
    ,error/2
    ,error/4
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

-record(state, {fd}).


%%%===================================================================
%%% API
%%%===================================================================

start_link(DirVar) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [DirVar], []).


%% 输出普通信息到日志文件
info(Msg) ->
    info(Msg, []).
info(Format, Args) ->
    info(Format, Args, null, null).
info(Format, Args, Mod, Line) ->
    Msg = format("info", Format, Args, Mod, Line),
    ?IF(erlang:whereis(?MODULE) =:= undefined, io:format("~ts", [Msg]), ?MODULE ! {log, Msg}).

%% 输出错误信息到日志文件
error(Msg) ->
    ?MODULE:error(Msg, []).
error(Format, Args) ->
    ?MODULE:error(Format, Args, null, null).
error(Format, Args, Mod, Line) ->
    Msg = format("error", Format, Args, Mod, Line),
    ?IF(erlang:whereis(?MODULE) =:= undefined, io:format("~ts", [Msg]), ?MODULE ! {log, Msg}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([DirVar]) ->
    process_flag(trap_exit, true),
    do_init(DirVar).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({log, Msg}, State = #state{fd = Fd}) ->
    io:format(Fd, "~s", [Msg]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{fd = Fd}) ->
    ?IF(Fd =/= undefined, file:close(Fd), skip),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

do_init(DirVar) ->
    case file_op(DirVar) of
        {ok, Fd} -> {ok, #state{fd = Fd}};
        Other -> Other
    end.

-ifdef(detached).
file_op(DirVar) ->
    filelib:ensure_dir(DirVar),
    Filename = DirVar ++ "mysql.log",
    file:open(Filename, [append]).
-else.
file_op(_DirVar) -> {ok, undefined}.
-endif.

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
