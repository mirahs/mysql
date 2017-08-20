-define(SECURE_CONNECTION, 32768).
-define(MYSQL_QUERY_OP, 3).
-define(DEFAULT_STANDALONE_TIMEOUT, 5000).
-define(MYSQL_4_0, 40). %% Support for MySQL 4.0.x
-define(MYSQL_4_1, 41). %% Support for MySQL 4.1.x et 5.0.x

-define(LONG_PASSWORD, 1).
-define(LONG_FLAG, 4).
-define(PROTOCOL_41, 512).
-define(TRANSACTIONS, 8192).
-define(CONNECT_WITH_DB, 8).
-define(MULTI_STATEMENTS, 	65536).
-define(MULTI_RESULTS, 		131072).

-define(MAX_PACKET_SIZE, 	1000000).


-define(MYSQL_CONNECT_NUM,			4).		% Mysql Pool默认连接数量
-define(MYSQL_TIMEOUT_RUN,          10).	% MySQL执行时间上限(秒) SideSrv
-define(MYSQL_HEART_TIME,			28000).	% MySQL心跳时间(秒)(mysql默认是8小时,这里差800秒8小时)


-record(mysql_result,	{
						 fieldinfo		= [],
						 rows			= [],
						 affectedrows	= 0,
						 insertid		= 0,
						 error			= "",
						 errcode		= 0,
						 errsqlstate	= ""
						}).

-record(mysql_pool, 	{
						 pool_id		= 0,	% 数据库连接池标识
					  	 pool_ready		= [], 	% [srv_name|...] mysql_side_srv进程注册名
						 pool_working	= [],	% [{Ref,FromPid,Time,SideSrvName}|...]当前正在工作的srv_name
						 queue			= []	% [{FromPid,Ref,Query}|...]等待查询的队列
					 	}).

-record(mysql_side,		{
						 pool_id= 0,
						 ver	= ?null,
						 socket = ?null,
						 bin	= <<>>,
						 srv_name,
					  	 host, port, username, password, database, charset, num
					 	}).
