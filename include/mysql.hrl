%%%===================================================================
%%% 日志记录
%%%===================================================================

-ifdef(debug).
-define(DEBUG(Msg), mysql_logger:debug(Msg, [], ?MODULE, ?LINE)).         % 输出调试信息
-define(DEBUG(F, A),mysql_logger:debug(F, A, ?MODULE, ?LINE)).
-else.
-define(DEBUG(Msg), ok).
-define(DEBUG(F, A),ok).
-endif.

-define(INFO(Msg),  catch mysql_logger:info(Msg, [], ?MODULE, ?LINE)).    % 输出普通信息
-define(INFO(F, A), catch mysql_logger:info(F, A, ?MODULE, ?LINE)).
-define(ERR(Msg),   catch mysql_logger:error(Msg, [], ?MODULE, ?LINE)).   % 输出错误信息
-define(ERR(F, A),  catch mysql_logger:error(F, A, ?MODULE, ?LINE)).


%%%===================================================================
%%% 函数封装
%%%===================================================================

-define(IF(B, T, F),case (B) of true -> (T); false -> (F) end).


%%%===================================================================
%%% MySQL 相关
%%%===================================================================

-define(SECURE_CONNECTION,	32768).
-define(MYSQL_QUERY_OP,		3).
-define(DEFAULT_STANDALONE_TIMEOUT, 5000).
-define(MYSQL_4_0,	40). %% Support for MySQL 4.0.x
-define(MYSQL_4_1,	41). %% Support for MySQL 4.1.x et 5.0.x

-define(LONG_PASSWORD, 1).
-define(LONG_FLAG, 4).
-define(PROTOCOL_41, 512).
-define(TRANSACTIONS, 8192).
-define(CONNECT_WITH_DB, 8).
-define(MULTI_STATEMENTS, 	65536).
-define(MULTI_RESULTS, 		131072).

-define(MAX_PACKET_SIZE, 	1000000).


-define(mysql_pool_conn_num,    4).		% MySQL Pool 连接数量
-define(mysql_query_timeout,    10).	% MySQL 查询时间上限(秒) SideSrv
-define(mysql_heart_time,       28200).	% MySQL 心跳时间(秒)(MySQL 默认是 8 小时, 这里差 600 秒 8 小时)


-record(mysql_result, {
    fieldinfo = [],
    rows = [],
    affectedrows = 0,
    insertid = 0,
    error = "",
    errcode = 0,
    errsqlstate	= ""
}).

-record(mysql_pool, {
    pool_id = 0,		% 数据库连接池标识
    pool_ready = [], 	% [srv_name | ...] mysql_side_srv 进程注册名
    pool_working = [],	% [{Ref, FromPid, Time, SideSrvName} | ...] 当前正在工作的srv_name
    queue = []			% [{FromPid, Ref, Query} | ...] 等待查询的队列
}).

-record(mysql_side, {
    pool_id = 0,
    ver	= null,
    socket = null,
    bin	= <<>>,
    srv_name,
    host, port, username, password, database, charset, num
}).
