-module(mysql_sup).

-behaviour(supervisor).

-export([
    start_link/0
]).

-export([
    init/1
]).


%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


%%%===================================================================
%%% supervisor callback functions
%%%===================================================================

init([]) ->
    MysqlSideSup= {mysql_side_sup,{mysql_side_sup, start_link, []},permanent, 5000, supervisor,	[mysql_side_sup]},
    MysqlSrv	= {mysql_srv, 		{mysql_srv, start_link, []}, 		permanent, 5000, worker,		[mysql_srv]},
    {ok, {{one_for_one , 10, 10}, [MysqlSideSup, MysqlSrv]} }.
