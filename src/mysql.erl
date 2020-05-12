-module(mysql).

-include("common.hrl").
-include("mysql.hrl").

-export([
	start/0,
	stop/0,

	add_pool/4,
	add_pool/5,
	add_pool/6,
	add_pool/7,
	add_pool/8,

	fetch/2,
	fetch/3,
	fetch_cast/2,
	fetch_cast/3,

	execute/2,
	execute/3,

	encode/1,
	encode/2,
	quote/1,
	format/2,

	insert/3,
	insert_cast/3,
	delete/2,
	delete/4,
	delete_cast/2,
	delete_cast/4,
	select/2,
	select/3,
	select_row/2,
	select_row/3,
	select_map/2,
	select_map/3,
	select_map_row/2,
	select_map_row/3,
	update/3,
	update/4,
	update/5,

	insert_execute/2,
	select_execute/2,
	update_execute/2
]).


%% @spec start() -> ok
%% @doc 启动mysql
start() ->
	application:start(mysql),
	ok.

%% @spec stop() -> ok
%% @doc 停止mysql
stop() ->
	application:stop(mysql),
	ok.


%% @spec add_pool(PoolId, Host, Port, Username, Password, Database, Charset, Num) -> ok
%% PoolId = integer()
%% Host = string()
%% Port = integer()
%% Username = string()
%% Password = string()
%% Database = string()
%% Charset = atom()
%% Num = integer()
%% @doc 添加数据库池
add_pool(PoolId, Username, Password, Database) ->
	add_pool(PoolId, "127.0.0.1", 3306, Username, Password, Database, utf8, ?MYSQL_CONNECT_NUM).
add_pool(PoolId, Username, Password, Database, Num) ->
	add_pool(PoolId, "127.0.0.1", 3306, Username, Password, Database, utf8, Num).

add_pool(PoolId, Host, Port, Username, Password, Database) ->
	add_pool(PoolId, Host, Port, Username, Password, Database, utf8, ?MYSQL_CONNECT_NUM).

add_pool(PoolId, Host, Port, Username, Password, Database, Num) when is_integer(Num) ->
	add_pool(PoolId, Host, Port, Username, Password, Database, utf8, Num);
add_pool(PoolId, Host, Port, Username, Password, Database, Charset) ->
	add_pool(PoolId, Host, Port, Username, Password, Database, Charset, ?MYSQL_CONNECT_NUM).

add_pool(PoolId, Host, Port, Username, Password, Database, Charset, Num) ->
	MysqlSide = #mysql_side{host = Host, port = Port, username = Username, password = Password, database = Database, charset = Charset, num = Num},
	mysql_srv:add_pool(PoolId, MysqlSide).


%% @spec fetch(PoolId, Query, Args) -> {ok, Result} | {error, Error}
%% PoolId = atom()
%% Query = string() | bitstring()
%% Args = list()
%% @doc 执行mysql语句(同步)
fetch(PoolId, Query) ->
	Pid	= self(),
	Ref	= make_ref(),
	mysql_srv:fetch(PoolId, Pid, Ref, Query),
	receive
		{?ok, Ref, Result} ->
			{?ok, Result};
		{error, Ref, Error} ->
			{error, Error}
	after ?MYSQL_TIMEOUT_RUN * 1000 ->
		{?error, timeout}
	end.
fetch(PoolId, QueryTmp, Args) ->
	Query = format(QueryTmp, Args),
	fetch(PoolId, Query).

%% @spec fetch_cast(PoolId, Query, Args) -> ok
%% PoolId = atom()
%% Query = string() | bitstring()
%% Args = list()
%% @doc 执行mysql语句(异步)
fetch_cast(PoolId, Query) ->
	Pid	= ?null,
	Ref	= make_ref(),
	mysql_srv:fetch(PoolId, Pid, Ref, Query),
	ok.
fetch_cast(PoolId, QueryTmp, Args) ->
	Query = format(QueryTmp, Args),
	fetch_cast(PoolId, Query).

%% @spec execute(PoolId, Query, Args) -> {ok, AffectedRows} | {error, Error}
%% PoolId = atom()
%% Query = string() | bitstring()
%% Args = list()
%% AffectedRows = integer()
%% Error = term()
%% @doc 执行SQL返回影响行数(insert,update)
execute(PoolId, Query) ->
	case mysql:fetch(PoolId, Query) of
		{?ok, #mysql_result{affectedrows = AffectedRows, error = ""}} ->
			{?ok, AffectedRows};
		{?ok, #mysql_result{error = Error}} ->
			{?error, Error};
		{?error, Error} ->
			{?error, Error}
	end.
execute(PoolId, QueryTmp, Args) ->
	Query = format(QueryTmp, Args),
	execute(PoolId, Query).


%% @spec insert(PoolId, Table, Datas) -> {ok, AffectedRows, LastInsertId} | {error, Error}
%% PoolId = atom()
%% Table = term()
%% Datas = list()
%% AffectedRows = integer()
%% LastInsertId = integer()
%% Error = term()
%% @doc 插入数据
%% mysql_api:insert(db_logic, gchy_mail, [{type,1},{send_uid,1}]).
insert(PoolId, Table, Datas) ->
	SQL = insert_encode(Table, Datas),
	insert_execute(PoolId, SQL).

%% @spec insert(PoolId, Table, Datas) -> ok
%% PoolId = atom()
%% Table = term()
%% Datas = list()
%% @doc 插入数据(异步)
insert_cast(PoolId, Table, Datas) ->
	Sql = insert_encode(Table, Datas),
	mysql:fetch_cast(PoolId, Sql).

%% @spec delete(PoolId, Table, Where, Args) -> {ok, AffectedRows} | {error, Error}
%% PoolId = atom()
%% Table = term()
%% Where = string() | bitstring()
%% Args = list()
%% AffectedRows = integer()
%% Error = term()
%% @doc 删除表数据
%% mysql_api:delete(db_logic, gchy_mail).
%% mysql_api:delete(db_logic, gchy_mail, "`id`=~s AND `age`=~s", [10,20]).
delete(PoolId, Table) ->
	delete(PoolId, Table, "", []).
delete(PoolId, Table, Where, Args) ->
	SQL = "DELETE FROM `" ++ mysql_util:to_list(Table) ++ "`" ++ ?IF(Where =:= "", ";", " WHERE " ++ Where),
	update_execute(PoolId, format(SQL, Args)).

%% @spec delete(PoolId, Table, Where, Args) -> ok
%% PoolId = atom()
%% Table = term()
%% Where = string() | bitstring()
%% Args = list()
%% AffectedRows = integer()
%% Error = term()
%% @doc 删除表数据
delete_cast(PoolId, Table)->
	delete_cast(PoolId, Table, "", []).
delete_cast(PoolId, Table, Where, Args) ->
	SQL = "DELETE FROM `" ++ mysql_util:to_list(Table) ++ "`" ++ ?IF(Where =:= "", ";", " WHERE " ++ Where),
	mysql:fetch_cast(PoolId, format(SQL, Args)).

%% @spec select(PoolId, Query, Args) -> {ok, Rows} | {error, Error}
%% PoolId = atom()
%% Query = string() | bitstring()
%% Args = list()
%% Rows = [list()|_]
%% Error = term()
%% @doc 查询数据
select(PoolId, SQL) ->
	select_execute(PoolId, SQL).
select(PoolId, SQL, Args) ->
	select_execute(PoolId, format(SQL, Args)).

%% @spec select_row(PoolId, Query, Args) -> {ok, Row} | null | {error, Error}
%% PoolId = atom()
%% Query = string() | bitstring()
%% Args = list()
%% Row = list()
%% Error = term()
%% @doc 查询数据(一行)
select_row(PoolId, SQL) ->
	case select_execute(PoolId, SQL) of
		{ok, [Row]} ->
			{ok, Row};
		{ok, []} ->
			null;
		{error, Error} ->
			{error, Error}
	end.
select_row(PoolId, SQL, Args) ->
	select_row(PoolId, format(SQL, Args)).
select_map(PoolId, SQL) ->
	select_execute_map(PoolId, SQL).
select_map(PoolId, SQL, Args) ->
	select_execute_map(PoolId, format(SQL, Args)).
select_map_row(PoolId, SQL) ->
	case select_execute_map(PoolId, SQL) of
		{ok, [Row]} ->
			{ok, Row};
		{ok, []} ->
			null;
		{error, Error} ->
			{error, Error}
	end.
select_map_row(PoolId, SQL, Args) ->
	select_map_row(PoolId, format(SQL, Args)).


%% @spec update(PoolId, Table, DataList, Where, Args, Limit) -> {ok, AffectedRows} | {error, Error}
%% PoolId = atom()
%% Table = term()
%% DataList = list()
%% Where = string() | bitstring()
%% Args = list()
%% Limit = integer()
%% AffectedRows = integer()
%% Error = term()
%% @doc 更新表数据
update(PoolId, Table, DataList) ->
	update(PoolId, Table, DataList, "", [], 0).
update(PoolId, Table, DataList, Where) ->
	update(PoolId, Table, DataList, Where, [], 0).
update(PoolId, Table, DataList, Where, Args) ->
	update(PoolId, Table, DataList, Where, Args, 0).
update(PoolId, Table, DataList, Where, Args, Limit) ->
	Limit2	= ?IF(Limit =:= 0, "", " LIMIT " ++ mysql_util:to_list(Limit)),
	WhereSql= ?IF(Where =:= "", "", " WHERE " ++ mysql_util:to_list(format(Where, Args))),
	DataStr	= update_encode(DataList),
	SQL		= "UPDATE `" ++ mysql_util:to_list(Table) ++ "` SET " ++ mysql_util:to_list(DataStr) ++ WhereSql ++ Limit2,
	update_execute(PoolId, SQL).


%% @spec insert_execute(PoolId, SQL) -> {ok, AffectedRows, LastInsertId} | {error, Error}
%% PoolId = atom()
%% SQL = string() | bitstring()
%% AffectedRows = integer()
%% LastInsertId = integer()
%% Error = term()
%% @doc 插入数据
insert_execute(PoolId, SQL)->
	case mysql:fetch(PoolId, SQL) of
		{?ok, #mysql_result{affectedrows=AffectedRows,insertid=LastInsertId,error=""}} ->
			{?ok, AffectedRows, LastInsertId};
		{?ok, #mysql_result{error=Error}} ->
			{?error, Error};
		{?error, Error} ->
			{?error, Error}
	end.

%% @spec update_execute(PoolId, SQL) -> {ok, AffectedRows} | {error, Error}
%% PoolId = atom()
%% SQL = string() | bitstring()
%% AffectedRows = integer()
%% Error = term()
%% @doc 更新数据
update_execute(PoolId, SQL) ->
	case mysql:fetch(PoolId, SQL) of
		{?ok, #mysql_result{affectedrows=AffectedRows,error=""}} ->
			{?ok, AffectedRows};
		{?ok, #mysql_result{error=Error}} ->
			{?error, Error};
		{?error, Error} ->
			{?error, Error}
	end.

%% @spec select_execute(PoolId, SQL) -> {ok, Datas} | {error, Error}
%% PoolId = atom()
%% SQL = string() | bitstring()
%% Datas = [list()|_]
%% Error = term()
%% @doc 查询数据
select_execute(PoolId, SQL) ->
	case mysql:fetch(PoolId, SQL) of
		{?ok, #mysql_result{rows=Data,error=""}} ->
			{?ok, Data};
		{?ok, #mysql_result{error=Error}} ->
			{?error, Error};
		{?error, Error} ->
			{?error, Error}
	end.
select_execute_map(PoolId, SQL) ->
	case mysql:fetch(PoolId, SQL) of
		{?ok, #mysql_result{fieldinfo=FieldInfo,rows=Data,error=""}} ->
			{?ok, to_map(FieldInfo, Data)};
		{?ok, #mysql_result{error=Error}} ->
			{?error, Error};
		{?error, Error} ->
			{?error, Error}
	end.



insert_encode(Table, [{_K,_V}|_] = Datas) ->
	{FieldList,DataList} = lists:unzip(Datas),
	FieldString = mysql_util:list_to_string(FieldList, "(`", "`,`", "`) "),
	Formats		= lists:duplicate(length(FieldList), "~s"),
	FormatString= mysql_util:list_to_string(Formats, " (", ",", ");"),
	SQL			= "INSERT INTO `" ++ mysql_util:to_list(Table) ++ "` " ++ FieldString ++ "VALUES" ++ FormatString,
	format(SQL, DataList);
insert_encode(Table, [[{_K,_V}|_] = First|_] = Datas) ->
	{FieldList,_} = lists:unzip(First),
	FieldString = mysql_util:list_to_string(FieldList, "(`", "`,`", "`) "),
	Formats		= lists:duplicate(length(FieldList), "~s"),
	FormatString= mysql_util:list_to_string(Formats, "", ",", ""),
	Fun = fun
			  (Data, ValuesAcc) ->
				  {_,DataList} = lists:unzip(Data),
				  Value = format(FormatString, DataList),
				  [Value|ValuesAcc]
		  end,
	Values = lists:reverse(lists:foldl(Fun, [], Datas)),
	ValuesString = mysql_util:list_to_string(Values, "(", "),(", ")"),
	"INSERT INTO `" ++ mysql_util:to_list(Table) ++ "` " ++ FieldString ++ "VALUES " ++ ValuesString ++ ";".

update_encode(Datas) ->
	{FieldList,DataList} = lists:unzip(Datas),
	FieldString = mysql_util:list_to_string(FieldList, "`", "`=~s,`", "`=~s"),
	format(FieldString, DataList).

to_map(FieldInfo, Rows) ->
	Fields = [mysql_util:to_atom(Field) || {_Table,Field,_Length,_Type} <- FieldInfo],
	to_map(Fields, Rows, []).

to_map(_Fields, [], Maps) ->
	lists:reverse(Maps);
to_map(Fields, [Row|Rows], Maps) ->
	Fun = fun
			  (Field, {IdxAcc,MapAcc}) ->
				  Val = lists:nth(IdxAcc, Row),
				  {IdxAcc + 1, MapAcc#{Field=>Val}}
		  end,
	{_Idx, Map} = lists:foldl(Fun, {1,#{}}, Fields),
	to_map(Fields, Rows, [Map|Maps]).



%% @doc Encode a value so that it can be included safely in a MySQL query.
%%
%% @spec encode(Val::term(), AsBinary::bool()) ->
%%   string() | binary() | {error, Error}
encode(Val) ->
	encode(Val, false).
encode(Val, false) when Val == undefined; Val == null ->
	"null";
encode(Val, true) when Val == undefined; Val == null ->
	<<"null">>;
encode(Val, false) when is_binary(Val) ->
	binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
	quote(Val);
encode(Val, true) ->
	list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
	quote(atom_to_list(Val));
encode(Val, false) when is_list(Val) ->
	quote(Val);
encode(Val, false) when is_integer(Val) ->
	integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
	[Res] = io_lib:format("~w", [Val]),
	Res;
encode({datetime, Val}, AsBinary) ->
	encode(Val, AsBinary);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
	Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
	lists:flatten(Res);
encode({TimeType, Val}, AsBinary)
	when TimeType == 'date';
	TimeType == 'time' ->
	encode(Val, AsBinary);
encode({Time1, Time2, Time3}, false) ->
	Res = two_digits([Time1, Time2, Time3]),
	lists:flatten(Res);
encode(Val, _AsBinary) ->
	{error, {unrecognized_value, Val}}.

two_digits(Nums) when is_list(Nums) ->
	[two_digits(Num) || Num <- Nums];
two_digits(Num) ->
	[Str] = io_lib:format("~b", [Num]),
	case length(Str) of
		1 -> [$0 | Str];
		_ -> Str
	end.

%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.
quote(String) when is_list(String) ->
	[39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
	list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
	Acc;
quote([0 | Rest], Acc) ->
	quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
	quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
	quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
	quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
	quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
	quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
	quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
	quote(Rest, [C | Acc]).


%% @spec format_sql(Sql, Args) -> bitstring().
%% Sql = string() | bitstring()
%% Args = list()
%% @doc 格式化SQL语句
format(Str, Args) when is_list(Str) ->
	Format = re:replace(Str, "\\?", "~s", [global, {return, list}]),
	L = [mysql:encode(A) || A <- Args],
	list_to_bitstring(io_lib:format(Format, L));
format(Str, Args) when is_bitstring(Str) ->
	format(bitstring_to_list(Str), Args).
