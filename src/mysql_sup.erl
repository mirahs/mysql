-module(mysql_sup).

-behaviour(supervisor).

-export([
	start_link/0
]).

-export([
	init/1
]).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
	%% 这两个进程要么保证都不会死掉，要么保证都死掉(之前的mysql链接进程都会清掉)
	MysqlSideSup= ?CHILD(mysql_side_sup,supervisor),
	MysqlSrv 	= ?CHILD(mysql_srv, 	worker),
    {ok, {{one_for_one, 1, 10}, [MysqlSideSup, MysqlSrv]} }.
