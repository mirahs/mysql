-module(mysql_auth).

-include("common.hrl").
-include("mysql.hrl").

-export([
		 auth/1
		]).


auth(#mysql_side{socket=Socket,username=Username,password=Password} = MysqlSide) ->
    case mysql_mod:do_recv(Socket, <<>>, ?undefined) of
		{ok, <<255:8, Rest/binary>>, _Data2, _InitSeqNum} ->
		    {_Code, ErrData} = get_error_data(Rest, ?MYSQL_4_0),
		    {error, ErrData};
		{ok, Packet, Data2, InitSeqNum} ->
		    {Version, Salt1, Salt2, Caps} = greeting(Packet),
		    AuthRes =
				case Caps band ?SECURE_CONNECTION of
				    ?SECURE_CONNECTION ->
						auth_new(Socket, Data2, InitSeqNum + 1,Username, Password, Salt1, Salt2);
				    _ ->
						auth_old(Socket, Data2, InitSeqNum + 1, Username, Password, Salt1)
				end,
		    case AuthRes of
				{ok, <<0:8, _Rest/binary>>, _Data3, _RecvNum} ->
				    {ok, MysqlSide#mysql_side{ver=Version}};
				{ok, <<255:8, Rest/binary>>, _Data3, _RecvNum} ->
				    {_Code, ErrData} = get_error_data(Rest, Version),
				    {error, ErrData};
				{ok, RecvPacket, _Data3, _RecvNum} ->
				    {error, binary_to_list(RecvPacket)};
				{error, Reason} ->
				    {error, Reason}
			    end;
		{?error, Reason} ->
		    {?error, Reason}
    end.


auth_old(Socket, Data, SeqNum, Username, Password, Salt1) ->
    Auth 	= password_old(Password, Salt1),
    Packet2 = make_auth(Username, Auth),
    mysql_mod:do_send(Socket, Packet2, SeqNum),
    mysql_mod:do_recv(Socket, Data, SeqNum).

auth_new(Socket, Data, SeqNum, Username, Password, Salt1, Salt2) ->
    Auth 	= password_new(Password, Salt1 ++ Salt2),
    Packet2 = make_new_auth(Username, Auth, none),
    mysql_mod:do_send(Socket, Packet2, SeqNum),
    case mysql_mod:do_recv(Socket, Data, SeqNum) of
	{ok, Packet3, Data2, SeqNum2} ->
	    case Packet3 of
		<<254:8>> ->
		    AuthOld = password_old(Password, Salt1),
		    mysql_mod:do_send(Socket, <<AuthOld/binary, 0:8>>, SeqNum2 + 1),
		    mysql_mod:do_recv(Socket, Data2, SeqNum2 + 1);
		_ ->
		    {ok, Packet3, Data2, SeqNum2}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.



hash(S) ->
    hash(S, 1345345333, 305419889, 7).

hash([C | S], N1, N2, Add) ->
    N1_1 = N1 bxor (((N1 band 63) + Add) * C + N1 * 256),
    N2_1 = N2 + ((N2 * 256) bxor N1_1),
    Add_1 = Add + C,
    hash(S, N1_1, N2_1, Add_1);
hash([], N1, N2, _Add) ->
    Mask = (1 bsl 31) - 1,
    {N1 band Mask , N2 band Mask}.

rnd(N, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    rnd(N, [], Seed1 rem Mod, Seed2 rem Mod).

rnd(0, List, _, _) ->
    lists:reverse(List);
rnd(N, List, Seed1, Seed2) ->
    Mod = (1 bsl 30) - 1,
    NSeed1 = (Seed1 * 3 + Seed2) rem Mod,
    NSeed2 = (NSeed1 + Seed2 + 33) rem Mod,
    Float = (float(NSeed1) / float(Mod))*31,
    Val = trunc(Float)+64,
    rnd(N - 1, [Val | List], NSeed1, NSeed2).

dualmap(_F, [], []) ->
    [];
dualmap(F, [E1 | R1], [E2 | R2]) ->
    [F(E1, E2) | dualmap(F, R1, R2)].

bxor_binary(B1, B2) ->
    list_to_binary(dualmap(fun (E1, E2) ->
                   E1 bxor E2
               end, binary_to_list(B1), binary_to_list(B2))).

password_old(Password, Salt) ->
    {P1, P2} = hash(Password),
    {S1, S2} = hash(Salt),
    Seed1 = P1 bxor S1,
    Seed2 = P2 bxor S2,
    List = rnd(9, Seed1, Seed2),
    {L, [Extra]} = lists:split(8, List),
    list_to_binary(lists:map(fun (E) ->
				     E bxor (Extra - 64)
			     end, L)).

make_auth(User, Password) ->
    Caps = ?LONG_PASSWORD bor ?LONG_FLAG bor ?TRANSACTIONS,
    Maxsize = 0,
    UserB = list_to_binary(User),
    PasswordB = Password,
    <<Caps:16/little, Maxsize:24/little, UserB/binary, 0:8,
    PasswordB/binary>>.

password_new([], _Salt) ->
    <<>>;
password_new(Password, Salt) ->
    %Stage1  = crypto:sha(Password),
    %Stage2  = crypto:sha(Stage1),
    Stage1  = crypto:hash(sha, Password),
    Stage2	= crypto:hash(sha, Stage1),

    %Salt1   = crypto:sha_update(crypto:sha_init(), Salt),
    %Salt2   = crypto:sha_update(Salt1, Stage2),
    %Res     = crypto:sha_final(Salt2),
    Salt1	= crypto:hash_update(crypto:hash_init(sha), Salt),
    Salt2	= crypto:hash_update(Salt1, Stage2),
    Res		= crypto:hash_final(Salt2),

    %Res     = crypto:sha_final(crypto:sha_update(crypto:sha_update(crypto:sha_init(), Salt),Stage2)),
    bxor_binary(Res, Stage1).

make_new_auth(User, Password, Database) ->
    DBCaps = case Database of
		 none ->
		     0;
		 _ ->
		     ?CONNECT_WITH_DB
	     end,
    %Caps = ?MYSQL_CLIENT_MULTI_STATEMENTS bor ?MYSQL_CLIENT_MULTI_RESULTS bor DBCaps,
    Caps = ?LONG_PASSWORD bor ?LONG_FLAG bor ?TRANSACTIONS bor ?PROTOCOL_41 bor ?SECURE_CONNECTION bor
        ?MULTI_STATEMENTS bor ?MULTI_RESULTS bor DBCaps,
    Maxsize = ?MAX_PACKET_SIZE,
    UserB = list_to_binary(User),
    PasswordL = size(Password),
    DatabaseB = case Database of
		    none ->
			<<>>;
		    _ ->
			list_to_binary(Database)
		end,
    <<Caps:32/little, Maxsize:32/little, 8:8, 0:23/integer-unit:8,
    UserB/binary, 0:8, PasswordL:8, Password/binary, DatabaseB/binary>>.





%% part of mysql_init/4
greeting(Packet) ->
    <<_Protocol:8, Rest/binary>> = Packet,
    {Version, Rest2} = asciz(Rest),
    <<_TreadID:32/little, Rest3/binary>> = Rest2,
    {Salt, Rest4} = asciz(Rest3),
    <<Caps:16/little, Rest5/binary>> = Rest4,
    <<_ServerChar:16/binary-unit:8, Rest6/binary>> = Rest5,
    {Salt2, _Rest7} = asciz(Rest6),
    {normalize_version(Version), Salt, Salt2, Caps}.

%% part of greeting/2
asciz(Data) when is_binary(Data) ->
    asciz_binary(Data, []);
asciz(Data) when is_list(Data) ->
    {String, [0 | Rest]} = lists:splitwith(fun (C) ->
						   C /= 0
					   end, Data),
    {String, Rest}.

normalize_version([$4,$.,$0|_T]) ->
    ?MYSQL_4_0;
normalize_version([$4,$.,$1|_T]) ->
    ?MYSQL_4_1;
normalize_version([$5|_T]) ->
    %% MySQL version 5.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
normalize_version(_Other) ->
    %% Error, but trying the oldest protocol anyway:
    ?MYSQL_4_0.


%% @doc Find the first zero-byte in Data and add everything before it
%%   to Acc, as a string.
%%
%% @spec asciz_binary(Data::binary(), Acc::list()) ->
%%   {NewList::list(), Rest::binary()}
asciz_binary(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
asciz_binary(<<0:8, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
asciz_binary(<<C:8, Rest/binary>>, Acc) ->
    asciz_binary(Rest, [C | Acc]).

get_error_data(<<Code:16/little, Message/binary>>, ?MYSQL_4_0) ->
    {Code, binary_to_list(Message)};
get_error_data(<<Code:16/little, _M:8, SqlState:5/binary, Message/binary>>, ?MYSQL_4_1) ->
    {Code, {binary_to_list(SqlState), binary_to_list(Message)}}.
