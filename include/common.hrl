%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 数据类型与常量
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-define(true, 					    true).		 %% true  真/开
-define(false, 					    false).		 %% false 假/关
-define(ok, 					    ok).	 	 %% ok
-define(error, 					    error).	 	 %% error
-define(start, 					    start).	 	 %% start
-define(stop, 					    stop).	 	 %% stop
-define(reply, 					    reply).	 	 %% reply
-define(noreply, 				    noreply).	 %% noreply
-define(timeout, 				    timeout).	 %% timeout
-define(ignore, 				    ignore).	 %% ignore
-define(skip,					    skip).	     %% skip
-define(undefined, 				    undefined).	 %% undefined
-define(null, 					    null).		 %% null
-define(normal, 				    normal).	 %% normal
-define(exit, 					    exit).	 	 %% exit
-define(trap_exit, 				    trap_exit).	 %% trap_exit


-define(IF(B,T,F), 				    case (B) of ?true -> (T); ?false -> (F) end).

-define(PRINT(Format),	        io:format("PRINT ~p:~p " ++ Format ++ "~n",			    [?MODULE,?LINE])			 ).
-define(PRINT(Format, Args),	io:format("PRINT ~p:~p " ++ Format ++ "~n",			    [?MODULE,?LINE|Args])			 ).
-define(ECHO(S),				io:format("ECHO Now:~p Pid:~p ~p:~p " ++ S ++ "~n",	    [erlang:localtime(),self(),?MODULE,?LINE])  ).
-define(ECHO(S, D),				io:format("ECHO Now:~p Pid:~p ~p:~p " ++ S ++ "~n",	    [erlang:localtime(),self(),?MODULE,?LINE|D])  ).
-define(ERROR(S),			    io:format("ERROR Now:~p Pid:~p ~p:~p " ++ S ++ "~n",	[erlang:localtime(),self(),?MODULE,?LINE])  ).
-define(ERROR(S, D),			io:format("ERROR Now:~p Pid:~p ~p:~p " ++ S ++ "~n",	[erlang:localtime(),self(),?MODULE,?LINE|D])  ).
