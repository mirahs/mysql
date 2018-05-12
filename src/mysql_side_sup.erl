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


start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child([]) -> ok;
start_child([ChildSpec | ChildSpecs]) ->
	case supervisor:start_child(?MODULE, ChildSpec) of
		{ok, _Pid} -> ok;
		{error, {already_started, _Pid}} -> ok
	end,
	start_child(ChildSpecs).

restart_child(SrvName)->
	supervisor:terminate_child(?MODULE, SrvName),
	supervisor:restart_child(?MODULE, 	SrvName),
	ok.


init([]) ->
	{ok, {{one_for_one, 10, 60}, []} }.
