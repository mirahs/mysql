-module(mysql).

-export([
    start/0
    ,stop/0

    ,add_pool/4
    ,add_pool/5
    ,add_pool/6
    ,add_pool/7
    ,add_pool/8

    ,fetch/2
    ,fetch/3
    ,fetch_cast/2
    ,fetch_cast/3

    ,execute/2
    ,execute/3

    ,insert/2
    ,insert/3
    ,insert_cast/3
    ,delete/2
    ,delete/4
    ,delete_cast/2
    ,delete_cast/4
    ,select/2
    ,select/3
    ,select_row/2
    ,select_row/3
    ,select_map/2
    ,select_map/3
    ,select_map_row/2
    ,select_map_row/3
    ,update/2
    ,update/3
    ,update/4
    ,update/5
    ,update/6

    ,encode/1
    ,encode/2
    ,quote/1
    ,format/2
]).

-include("mysql.hrl").


%%%===================================================================
%%% API
%%%===================================================================

%% 启动mysql
-spec start() -> ok.
start() ->
    {ok, _} = application:ensure_all_started(mysql),
    ok.

%% 停止mysql
-spec stop() -> ok.
stop() ->
    application:stop(mysql),
    ok.


%% 添加数据库链接池
-spec add_pool(atom(), string(), string(), string()) -> ok.
add_pool(PoolId, Username, Password, Database) ->
    add_pool(PoolId, "127.0.0.1", 3306, Username, Password, Database, utf8mb4, ?mysql_pool_conn_num).

-spec add_pool(atom(), string(), string(), string(), non_neg_integer()) -> ok.
add_pool(PoolId, Username, Password, Database, Num) ->
    add_pool(PoolId, "127.0.0.1", 3306, Username, Password, Database, utf8mb4, Num).

-spec add_pool(atom(), string(), non_neg_integer(), string(), string(), string()) -> ok.
add_pool(PoolId, Host, Port, Username, Password, Database) ->
    add_pool(PoolId, Host, Port, Username, Password, Database, utf8mb4, ?mysql_pool_conn_num).

-spec add_pool(atom(), string(), non_neg_integer(), string(), string(), string(), non_neg_integer() | string()) -> ok.
add_pool(PoolId, Host, Port, Username, Password, Database, Num) when is_integer(Num) ->
    add_pool(PoolId, Host, Port, Username, Password, Database, utf8mb4, Num);
add_pool(PoolId, Host, Port, Username, Password, Database, Charset) ->
    add_pool(PoolId, Host, Port, Username, Password, Database, Charset, ?mysql_pool_conn_num).

-spec add_pool(atom(), string(), non_neg_integer(), string(), string(), string(), string(), non_neg_integer()) -> ok.
add_pool(PoolId, Host, Port, Username, Password, Database, Charset, Num) ->
    MysqlSide = #mysql_side{host = Host, port = Port, username = Username, password = Password, database = Database, charset = Charset, num = Num},
    mysql_srv:add_pool(PoolId, MysqlSide).


%% 执行 SQL 语句(同步)
-spec fetch(atom(), string() | binary()) -> {ok, #mysql_result{}} | {error, any()}.
fetch(PoolId, Query) ->
    Pid	= self(),
    Ref	= make_ref(),
    mysql_srv:fetch(PoolId, Pid, Ref, Query),
    receive
        {ok, Ref, Result} -> {ok, Result};
        {error, Ref, Error} -> {error, Error}
    after ?mysql_query_timeout * 1000 -> {error, timeout}
    end.
-spec fetch(atom(), string() | binary(), [any()]) -> {ok, #mysql_result{}} | {error, any()}.
fetch(PoolId, QueryTmp, Args) ->
    Query = format(QueryTmp, Args),
    fetch(PoolId, Query).

%% 执行 SQL 语句(异步)
-spec fetch_cast(atom(), string() | binary()) -> ok.
fetch_cast(PoolId, Query) ->
    Pid	= null,
    Ref	= make_ref(),
    mysql_srv:fetch(PoolId, Pid, Ref, Query),
    ok.
-spec fetch_cast(atom(), string() | binary(), [any()]) -> ok.
fetch_cast(PoolId, QueryTmp, Args) ->
    Query = format(QueryTmp, Args),
    fetch_cast(PoolId, Query).


%% 执行 SQL 并返回影响行数(insert,update)
-spec execute(atom(), string() | binary()) -> {ok, non_neg_integer()} | {error, any()}.
execute(PoolId, Query) ->
    case mysql:fetch(PoolId, Query) of
        {ok, #mysql_result{affectedrows = AffectedRows, error = ""}} -> {ok, AffectedRows};
        {ok, #mysql_result{error = Error}} -> {error, Error};
        {error, Error} -> {error, Error}
    end.
-spec execute(atom(), string() | binary(), [any()]) -> {ok, non_neg_integer()} | {error, any()}.
execute(PoolId, QueryTmp, Args) ->
    Query = format(QueryTmp, Args),
    execute(PoolId, Query).


%% 插入数据
-spec insert(atom(), string() | binary()) -> {ok, non_neg_integer(), non_neg_integer()} | {error, any()}.
insert(PoolId, Query) ->
    execute_insert(PoolId, Query).
%% mysql:insert(db_core, user, [{username, "admin"}, {password, "admin"}, {age, 18}]).
-spec insert(atom(), atom(), [{atom(), any()}]) -> {ok, non_neg_integer(), non_neg_integer()} | {error, any()}.
insert(PoolId, Table, Datas) ->
    SQL = format_insert(Table, Datas),
    execute_insert(PoolId, SQL).

%% 插入数据(异步)
-spec insert_cast(atom(), atom(), [{atom(), any()}]) -> {ok, non_neg_integer(), non_neg_integer()} | {error, any()}.
insert_cast(PoolId, Table, Datas) ->
    Sql = format_insert(Table, Datas),
    mysql:fetch_cast(PoolId, Sql).

%% 删除数据
%% mysql:delete(db_core, user).
-spec delete(atom(), atom()) -> {ok, non_neg_integer()} | {error, any()}.
delete(PoolId, Table) ->
    delete(PoolId, Table, "", []).
%% mysql:delete(db_core, user, "`id`=~s AND `age`=~s", [10,20]).
-spec delete(atom(), atom(), string() | binary(), list()) -> {ok, non_neg_integer()} | {error, any()}.
delete(PoolId, Table, Where, Args) ->
    SQL = "DELETE FROM `" ++ mysql_util:to_list(Table) ++ "`" ++ ?IF(Where =:= "", ";", " WHERE " ++ Where),
    execute(PoolId, format(SQL, Args)).

%% 删除数据(异步)
-spec delete_cast(atom(), atom()) -> ok.
delete_cast(PoolId, Table)->
    delete_cast(PoolId, Table, "", []).
-spec delete_cast(atom(), atom(), string() | binary(), list()) -> ok.
delete_cast(PoolId, Table, Where, Args) ->
    SQL = "DELETE FROM `" ++ mysql_util:to_list(Table) ++ "`" ++ ?IF(Where =:= "", ";", " WHERE " ++ Where),
    mysql:fetch_cast(PoolId, format(SQL, Args)).

%% 查询数据
-spec select(atom(), string() | binary()) -> {ok, [list()]} | {error, any()}.
select(PoolId, SQL) ->
    execute_select(PoolId, SQL).
-spec select(atom(), string() | binary(), [list()]) -> {ok, list()} | {error, any()}.
select(PoolId, SQL, Args) ->
    execute_select(PoolId, format(SQL, Args)).

%% 查询数据(一行)
-spec select_row(atom(), string() | binary()) -> {ok, list()} | null | {error, any}.
select_row(PoolId, SQL) ->
    case execute_select(PoolId, SQL) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> null;
        {error, Error} -> {error, Error}
    end.
-spec select_row(atom(), string() | binary(), list()) -> {ok, list()} | null | {error, any}.
select_row(PoolId, SQL, Args) ->
    select_row(PoolId, format(SQL, Args)).
-spec select_map(atom(), string() | binary()) -> {ok, [map()]} | null | {error, any}.
select_map(PoolId, SQL) ->
    execute_select_map(PoolId, SQL).
-spec select_map(atom(), string() | binary(), list()) -> {ok, [map()]} | null | {error, any}.
select_map(PoolId, SQL, Args) ->
    execute_select_map(PoolId, format(SQL, Args)).
-spec select_map_row(atom(), string() | binary()) -> {ok, map()} | null | {error, any}.
select_map_row(PoolId, SQL) ->
    case execute_select_map(PoolId, SQL) of
        {ok, [Row]} -> {ok, Row};
        {ok, []} -> null;
        {error, Error} -> {error, Error}
    end.
-spec select_map_row(atom(), string() | binary(), list()) -> {ok, map()} | null | {error, any}.
select_map_row(PoolId, SQL, Args) ->
    select_map_row(PoolId, format(SQL, Args)).


%% 更新表数据
-spec update(atom(), string() | binary()) -> {ok, non_neg_integer()} | {error, any()}.
update(PoolId, Query) ->
    execute(PoolId, Query).
-spec update(atom(), string() | binary(), [{atom(), any()}]) -> {ok, non_neg_integer()} | {error, any()}.
update(PoolId, Table, DataList) ->
    update(PoolId, Table, DataList, "", [], 0).
-spec update(atom(), string() | binary(), [{atom(), any()}], string() | binary()) -> {ok, non_neg_integer()} | {error, any()}.
update(PoolId, Table, DataList, Where) ->
    update(PoolId, Table, DataList, Where, [], 0).
-spec update(atom(), string() | binary(), [{atom(), any()}], string() | binary(), [any()]) -> {ok, non_neg_integer()} | {error, any()}.
update(PoolId, Table, DataList, Where, Args) ->
    update(PoolId, Table, DataList, Where, Args, 0).
-spec update(atom(), string() | binary(), [{atom(), any()}], string() | binary(), [any()], non_neg_integer()) -> {ok, non_neg_integer()} | {error, any()}.
update(PoolId, Table, DataList, Where, Args, Limit) ->
    Limit2	= ?IF(Limit =:= 0, "", " LIMIT " ++ mysql_util:to_list(Limit)),
    WhereSql= ?IF(Where =:= "", "", " WHERE " ++ mysql_util:to_list(format(Where, Args))),
    DataStr	= format_update(DataList),
    SQL		= "UPDATE `" ++ mysql_util:to_list(Table) ++ "` SET " ++ DataStr ++ WhereSql ++ Limit2,
    execute(PoolId, SQL).


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

%% 格式化 SQL 语句
-spec format(string() | binary(), [term()]) -> binary().
format(Format0, Args) when is_list(Format0) ->
    Format = re:replace(Format0, "\\?", "~s", [global, {return, list}]),
    L = [encode(A) || A <- Args],
    io_lib:format(Format, L);
format(Format0, Args) when is_binary(Format0) ->
    list_to_binary(format(binary_to_list(Format0), Args)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

execute_insert(PoolId, SQL)->
    case mysql:fetch(PoolId, SQL) of
        {ok, #mysql_result{affectedrows = AffectedRows, insertid = LastInsertId, error = ""}} -> {ok, AffectedRows, LastInsertId};
        {ok, #mysql_result{error = Error}} -> {error, Error};
        {error, Error} -> {error, Error}
    end.

execute_select(PoolId, SQL) ->
    case mysql:fetch(PoolId, SQL) of
        {ok, #mysql_result{rows = Data, error = ""}} -> {ok, Data};
        {ok, #mysql_result{error = Error}} -> {error, Error};
        {error, Error} -> {error, Error}
    end.

execute_select_map(PoolId, SQL) ->
    case mysql:fetch(PoolId, SQL) of
        {ok, #mysql_result{fieldinfo = FieldInfo, rows = Data, error = ""}} -> {ok, to_map(FieldInfo, Data)};
        {ok, #mysql_result{error = Error}} -> {error, Error};
        {error, Error} -> {error, Error}
    end.


format_insert(Table, Datas = [{_K, _V} | _]) ->
    {FieldList, DataList} = lists:unzip(Datas),
    FieldString = mysql_util:list_to_string(FieldList, "(`", "`,`", "`) "),
    Formats		= lists:duplicate(length(FieldList), "~s"),
    FormatString= mysql_util:list_to_string(Formats, " (", ",", ");"),
    SQL			= "INSERT INTO `" ++ mysql_util:to_list(Table) ++ "` " ++ FieldString ++ "VALUES" ++ FormatString,
    format(SQL, DataList);
format_insert(Table, Datas = [[{_K, _V} | _] = First | _]) ->
    {FieldList, _} = lists:unzip(First),
    FieldString = mysql_util:list_to_string(FieldList, "(`", "`,`", "`) "),
    Formats		= lists:duplicate(length(FieldList), "~s"),
    FormatString= mysql_util:list_to_string(Formats, "", ",", ""),
    Fun = fun
              (Data, ValuesAcc) ->
                  {_, DataList} = lists:unzip(Data),
                  Value = format(FormatString, DataList),
                  [Value | ValuesAcc]
          end,
    Values = lists:reverse(lists:foldl(Fun, [], Datas)),
    ValuesString = mysql_util:list_to_string(Values, "(", "),(", ")"),
    "INSERT INTO `" ++ mysql_util:to_list(Table) ++ "` " ++ FieldString ++ "VALUES " ++ ValuesString ++ ";".

format_update(Datas) ->
    {FieldList, DataList} = lists:unzip(Datas),
    FieldString = mysql_util:list_to_string(FieldList, "`", "`=~s,`", "`=~s"),
    format(FieldString, DataList).


%% 数据转成 map
to_map(Fields0, Datas) ->
    Fields = [mysql_util:to_atom(Field) || {_Table, Field, _Length, _Type} <- Fields0],
    [maps:from_list(lists:zip(Fields, Data)) || Data <- Datas].
