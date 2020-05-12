-module(mysql_util).

-include("common.hrl").

-export([
    to_atom/1,
    to_binary/1,
    to_float/1,
    to_integer/1,
    to_list/1,
    to_tuple/1,

    now/0,
    milliseconds/0,
    seconds/0,

    list_to_string/4,
    list_to_atom/1,

    core_count/0
]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 各种转换
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% atom
to_atom(Msg) when is_atom(Msg) ->
    Msg;
to_atom(Msg) when is_binary(Msg) ->
    ?MODULE:list_to_atom(binary_to_list(Msg));
to_atom(Msg) when is_integer(Msg) ->
    ?MODULE:list_to_atom(integer_to_list(Msg));
to_atom(Msg) when is_tuple(Msg) ->
    ?MODULE:list_to_atom(tuple_to_list(Msg));
to_atom(Msg) when is_list(Msg) ->
    Msg2 = list_to_binary(Msg),
    Msg3 = binary_to_list(Msg2),
    ?MODULE:list_to_atom(Msg3);
to_atom(_) ->
    ?MODULE:list_to_atom("").

%% list
to_list(Msg) when is_list(Msg) ->
    Msg;
to_list(Msg) when is_atom(Msg) ->
    atom_to_list(Msg);
to_list(Msg) when is_binary(Msg) ->
    binary_to_list(Msg);
to_list(Msg) when is_integer(Msg) ->
    integer_to_list(Msg);
to_list(Msg) when is_float(Msg) ->
    float_to_list(Msg);
to_list(_) ->
    [].

%% binary
to_binary(Msg) when is_binary(Msg) ->
    Msg;
to_binary(Msg) when is_atom(Msg) ->
    list_to_binary(atom_to_list(Msg));
to_binary(Msg) when is_list(Msg) ->
    list_to_binary(Msg);
to_binary(Msg) when is_integer(Msg) ->
    list_to_binary(integer_to_list(Msg));
to_binary(_Msg) ->
    <<>>.

%% float
to_float(Msg)->
    Msg2 = to_list(Msg),
    list_to_float(Msg2).

%% integer
to_integer(Msg) when is_integer(Msg) ->
    Msg;
to_integer(Msg) when is_binary(Msg) ->
    Msg2 = binary_to_list(Msg),
    list_to_integer(Msg2);
to_integer(Msg) when is_list(Msg) ->
    list_to_integer(Msg);
to_integer(_Msg) ->
    0.

%% tuple
to_tuple(T) when is_tuple(T) ->
    T;
to_tuple(T) ->
    {T}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 时间日期
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
now() ->
    erlang:timestamp().

milliseconds() ->
    erlang:system_time(milli_seconds).

%% 1970年1月1日到现在的秒数
seconds() ->
    erlang:system_time(seconds).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 系统加强
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 数组转成字符串
%% List -> String
%% H 附加在开头
%% M 夹在中间
%% T 附加在尾部
list_to_string([], _H, _M, _T) ->
    [];
list_to_string([HList|TList], H, M, T) ->
    list_to_string(TList, H, M, T, H ++ to_list(HList)).

list_to_string([], _H, _M, T, Str) ->
    Str ++ T;
list_to_string([HList|TList], H, M, T, Str) ->
    list_to_string(TList, H, M, T, Str ++ M ++ to_list(HList)).

%% list_to_existing_atom
list_to_atom(List)->
    try
        erlang:list_to_existing_atom(List)
    catch _:_ ->
        erlang:list_to_atom(List)
    end.


%% cpu核数
core_count() ->
    erlang:system_info(schedulers).
