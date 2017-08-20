-module(mysql_mod).

-include("common.hrl").
-include("mysql.hrl").

-export([
		 do_recv/3,
		 do_send/3,
		 do_query/4
		]).


do_recv(_Socket, <<Length:24/little, Num:8, Packet:Length/binary, Rest/binary>>, SeqNum) ->
	SeqNumNew = seq_num(SeqNum, Num),
	{?ok, Packet, Rest, SeqNumNew};
do_recv(Socket, Data, SeqNum) ->
	receive
		{tcp, Socket, InData} ->
			do_recv(Socket,<<Data/binary,InData/binary>>,SeqNum);
		{tcp_error, Socket, Reason} ->
			?ERROR("mysql_recv: Socket ~p closed: ~p", [Socket,Reason]),
	    	gen_tcp:close(Socket),
			{?error, tcp_error}; 
		{tcp_closed, Socket} ->
	    	?ERROR("mysql_recv: Socket ~p closed", [Socket]),
	    	gen_tcp:close(Socket),
			{?error, tcp_closed};
		close ->
			?ERROR("smysql_recv: socket closed", []),
			gen_tcp:close(Socket),
			{?error, closed}
	end.

do_send(Socket, Packet, Num) ->
    Data = <<(size(Packet)):24/little, Num:8, Packet/binary>>,
    gen_tcp:send(Socket, Data).

do_query(Socket, Bin, Version, Query) ->
	%?MSG_ECHO("do_query Query: ~p~n", [Query]),
	Query2 = iolist_to_binary(Query),
    Packet = <<?MYSQL_QUERY_OP, Query2/binary>>,
    case do_send(Socket, Packet, 0) of
		ok ->
		    get_query_response(Socket, Bin, Version);
		{error, Reason} ->
		    Msg = io_lib:format("Failed sending data on socket : ~p", [Reason]),
		    {error, Msg}
    end.


seq_num(?undefined, Num) ->
	Num;
seq_num(SeqNum, Num) ->
	Num = SeqNum + 1, % 这里一定要匹配
	Num.

get_query_response(Socket, Bin, Version) ->
	%?MSG_ECHO("get_query_response Bin: ~p~n", [Bin]),
	case do_recv(Socket, Bin, ?undefined) of
		{ok, Packet, Bin2, _} ->
			{Fieldcount, Rest} = get_lcb(Packet),
			case Fieldcount of
				0 ->
					%% No Tabular data
					{AffectedRows, Rest2} = get_lcb(Rest),
					{InsertId, _} = get_lcb(Rest2),
					{?ok, #mysql_result{affectedrows=AffectedRows, insertid=InsertId}, Bin2};
				255 ->
					%?MSG_ECHO("has a error!"),
					case get_error_data(Rest, Version) of
						{Code, {SqlState, Message}} ->
							% MYSQL_4_1 error data
							{?ok, #mysql_result{error=Message, errcode=Code, errsqlstate=SqlState}, Bin2};
						{Code, Message} ->
							% MYSQL_4_0 error data
							{?ok, #mysql_result{error=Message, errcode=Code}, Bin2}
					end;
				_ ->
					%% Tabular data received
					case get_fields(Socket, Bin2, [], Version) of
						{ok, Fields, Bin3} ->
							case get_rows(Fields, Socket, Bin3, [], Version) of
								{ok, Rows, Bin4} ->
									{ok, #mysql_result{fieldinfo=Fields, rows=Rows}, Bin4};
								{error, {Code, {SqlState, Message}}, Bin4} ->
									% MYSQL_4_1 error data
									{?ok, #mysql_result{error=Message, errcode=Code, errsqlstate=SqlState}, Bin4};
								{error, {Code, Message}, Bin4} ->
									% MYSQL_4_0 error data
									{?ok, #mysql_result{error=Message, errcode=Code}, Bin4};
								{error, Reason} ->
									{error, Reason}
							end;
						{error, Reason} ->
							{error, Reason}
					end
			end;
		{error, Reason} ->
			{error, Reason}
	end.

%% Support for MySQL 4.0.x:
get_fields(Socket, Bin, Res, ?MYSQL_4_0 = Version) ->
    case do_recv(Socket, Bin, undefined) of
	{ok, Packet, Bin2, _Num} ->
	    case Packet of
			<<254:8>> ->
			    {ok, lists:reverse(Res), Bin2};
			<<254:8, Rest/binary>> when size(Rest) < 8 ->
			    {ok, lists:reverse(Res), Bin2};
			_ ->
			    {Table, Rest} = get_with_length(Packet),
			    {Field, Rest2} = get_with_length(Rest),
			    {LengthB, Rest3} = get_with_length(Rest2),
			    LengthL = size(LengthB) * 8,
			    <<Length:LengthL/little>> = LengthB,
			    {Type, Rest4} = get_with_length(Rest3),
			    {_Flags, _Rest5} = get_with_length(Rest4),
			    This = {Table,
				    Field,
				    Length,
				    %% TODO: Check on MySQL 4.0 if types are specified
				    %%       using the same 4.1 formalism and could 
				    %%       be expanded to atoms:
				    Type},
			    get_fields(Socket, Bin2, [This | Res], Version)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end;
%% Support for MySQL 4.1.x and 5.x:
get_fields(Socket, Bin, Res, ?MYSQL_4_1 = Version) ->
    case do_recv(Socket, Bin, undefined) of
	{ok, Packet, Bin2, _Num} ->
	    case Packet of
		<<254:8>> ->
		    {ok, lists:reverse(Res), Bin2};
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res), Bin2};
		_ ->
		    {_Catalog, Rest} = get_with_length(Packet),
		    {_Database, Rest2} = get_with_length(Rest),
		    {Table, Rest3} = get_with_length(Rest2),
		    %% OrgTable is the real table name if Table is an alias
		    {_OrgTable, Rest4} = get_with_length(Rest3),
		    {Field, Rest5} = get_with_length(Rest4),
		    %% OrgField is the real field name if Field is an alias
		    {_OrgField, Rest6} = get_with_length(Rest5),

		    <<_Metadata:8/little, _Charset:16/little,
		     Length:32/little, Type:8/little,
		     _Flags:16/little, _Decimals:8/little,
		     _Rest7/binary>> = Rest6,
		    
		    This = {Table,
			    Field,
			    Length,
			    get_field_datatype(Type)},
		    get_fields(Socket, Bin2, [This | Res], Version)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

get_rows(Fields, Socket, Bin, Res, Version) ->
    case do_recv(Socket, Bin, undefined) of
	{ok, Packet, Bin2, _Num} ->
	    case Packet of
			<<254:8, Rest/binary>> when size(Rest) < 8 ->
			    {ok, lists:reverse(Res), Bin2};
			<<255:8, Rest/binary>> ->
			    {Code, ErrData} = get_error_data(Rest, Version),		    
			    {error, {Code, ErrData}, Bin2};
			_ ->
			    {ok, This} = get_row(Fields, Packet, []),
			    get_rows(Fields, Socket, Bin2, [This | Res], Version)
		    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%% part of get_rows/4
get_row([], _Data, Res) ->
    {ok, lists:reverse(Res)};
get_row([Field | OtherFields], Data, Res) ->
    {Col, Rest} = get_with_length(Data),
    This = case Col of
	       ?null ->
	       	?undefined;
	       _ ->
		   convert_type(Col, element(4, Field))
	   end,
    get_row(OtherFields, Rest, [This | Res]).

get_with_length(Bin) when is_binary(Bin) ->
    {Length, Rest} = get_lcb(Bin),
    case get_lcb(Bin) of 
    	 {null, Rest} -> {null, Rest};
    	 _ -> split_binary(Rest, Length)
    end.


get_lcb(<<251:8, Rest/binary>>) ->
    {null, Rest};
get_lcb(<<252:8, Value:16/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<253:8, Value:24/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<254:8, Value:32/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<Value:8, Rest/binary>>) when Value < 251 ->
    {Value, Rest};
get_lcb(<<255:8, Rest/binary>>) ->
    {255, Rest}.

convert_type(Val, ColType) ->
    case ColType of
	T when T == 'TINY';
	       T == 'SHORT';
	       T == 'LONG';
	       T == 'LONGLONG';
	       T == 'INT24';
	       T == 'YEAR' ->
	    list_to_integer(binary_to_list(Val));
	T when T == 'TIMESTAMP';
	       T == 'DATETIME' ->
	    {ok, [Year, Month, Day, Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d-~d-~d ~d:~d:~d", binary_to_list(Val)),
	    {datetime, {{Year, Month, Day}, {Hour, Minute, Second}}};
	'TIME' ->
	    {ok, [Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d:~d:~d", binary_to_list(Val)),
	    {time, {Hour, Minute, Second}};
	'DATE' ->
	    {ok, [Year, Month, Day], _Leftovers} =
		io_lib:fread("~d-~d-~d", binary_to_list(Val)),
	    {date, {Year, Month, Day}};
	T when T == 'DECIMAL';
	       T == 'NEWDECIMAL';
	       T == 'FLOAT';
	       T == 'DOUBLE' ->
	    {ok, [Num], _Leftovers} =
		case io_lib:fread("~f", binary_to_list(Val)) of
		    {error, _} ->
			io_lib:fread("~d", binary_to_list(Val));
		    Res ->
			Res
		end,
	    Num;
	_Other ->
	    Val
    end.

get_error_data(ErrPacket, ?MYSQL_4_0) ->
    <<Code:16/little, Message/binary>> = ErrPacket,
    {Code, binary_to_list(Message)};
get_error_data(ErrPacket, ?MYSQL_4_1) ->
    <<Code:16/little, _M:8, SqlState:5/binary, Message/binary>> = ErrPacket,
    {Code, {binary_to_list(SqlState), binary_to_list(Message)}}.

%%--------------------------------------------------------------------
% Function: get_field_datatype(DataType)
%%           DataType = integer(), MySQL datatype
%% Descrip.: Return MySQL field datatype as description string
%% Returns : String, MySQL datatype
%%--------------------------------------------------------------------
get_field_datatype(0) ->   'DECIMAL';
get_field_datatype(1) ->   'TINY';
get_field_datatype(2) ->   'SHORT';
get_field_datatype(3) ->   'LONG';
get_field_datatype(4) ->   'FLOAT';
get_field_datatype(5) ->   'DOUBLE';
get_field_datatype(6) ->   'NULL';
get_field_datatype(7) ->   'TIMESTAMP';
get_field_datatype(8) ->   'LONGLONG';
get_field_datatype(9) ->   'INT24';
get_field_datatype(10) ->  'DATE';
get_field_datatype(11) ->  'TIME';
get_field_datatype(12) ->  'DATETIME';
get_field_datatype(13) ->  'YEAR';
get_field_datatype(14) ->  'NEWDATE';
get_field_datatype(246) -> 'NEWDECIMAL';
get_field_datatype(247) -> 'ENUM';
get_field_datatype(248) -> 'SET';
get_field_datatype(249) -> 'TINYBLOB';
get_field_datatype(250) -> 'MEDIUM_BLOG';
get_field_datatype(251) -> 'LONG_BLOG';
get_field_datatype(252) -> 'BLOB';
get_field_datatype(253) -> 'VAR_STRING';
get_field_datatype(254) -> 'STRING';
get_field_datatype(255) -> 'GEOMETRY'.
