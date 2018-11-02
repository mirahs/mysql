-module(mysql_side_sup).

-behaviour(supervisor).

-export([
	start_link/0

	,start_child/1
	,restart_child/1
]).

-export([
	init/1
]).

-include("common.hrl").


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child([]) -> ok;
start_child([ChildSpec | ChildSpecs]) ->
	case supervisor:start_child(?MODULE, ChildSpec) of
		{ok, _Pid} -> ok;
		{error, {already_started, _Pid}} -> ok;
		{error, Error} ->
			?ERROR("ChildSpec: ~p Error: ~p", [ChildSpec, Error])
	end,
	start_child(ChildSpecs).

restart_child(SrvName)->
	supervisor:terminate_child(?MODULE, SrvName),
	supervisor:restart_child(?MODULE, 	SrvName),
	ok.


init([]) ->
	%% 当mysql链接进程第一次成功启动，如果以后挂掉的话，则在一天的时间里，重连1440次，即每60秒重连一次
	{ok, {{one_for_one, 1440, 86400}, []} }.
