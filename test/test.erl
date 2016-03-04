-module(test).
-include_lib("eunit/include/eunit.hrl").
-define(CFG,#{wthreads => 3, startindex => {1}, paths => {"./"}, pwrite => 0}).
-define(INIT,aqdrv:init(?CFG)).
-define(LOAD_TEST_COMPR,false).

run_test_() ->
	% [file:delete(Fn) || Fn <- ["1"]],
	% Write random data over beginning
	{ok,W} = file:open("1",[write,read,binary,raw]),
	file:write(W,crypto:rand_bytes(400096)),
	file:close(W),
	[
	fun dowrite/0,
	% fun cleanup/0
	{timeout,50,fun async/0}
	].


dowrite() ->
	?INIT,
	application:ensure_all_started(crypto),
	C = aqdrv:open(1,true),
	Header = [<<"HEADER_PART1">>,<<"HEADER_PART2">>],
	HeaderSz = iolist_size(Header),
	Body = <<"DATA SECTION START",(crypto:rand_bytes(4096))/binary>>,
	ok = aqdrv:stage_map(C, <<"ITEM1">>, 1, byte_size(Body)),
	ok = aqdrv:stage_data(C, Body),
	{MapSize, DataSize} = aqdrv:stage_flush(C),
	?debugFmt("Mapsize ~p, datasize ~p",[MapSize, DataSize]),
	WPos = aqdrv:write(C, [<<"WILL BE IGNORED">>], Header),
	?debugFmt("Wpos ~p",[WPos]),
	
	Body2 = <<"AAABBBBCCCCDDDDEEEEFFFFF">>,
	ok = aqdrv:stage_map(C, <<"ITEM2">>, 1, byte_size(Body2)),
	ok = aqdrv:stage_data(C, Body2),
	{MapSize1, DataSize1} = aqdrv:stage_flush(C),
	?debugFmt("Mapsize ~p, datasize ~p",[MapSize1, DataSize1]),
	WPos1 = aqdrv:write(C, [<<"WILL BE IGNORED">>], Header),
	?debugFmt("Wpos ~p",[WPos1]),

	{ok,F} = file:open("1",[read,binary,raw]),
	{ok,Bin} = file:read(F,1024*1024),
	<<(16#184D2A50):32/unsigned-little, HeaderSz:32/unsigned-little,
		"HEADER_PART1","HEADER_PART2",
	  (16#184D2204):32/unsigned-little,_/binary>> = Bin,
	file:close(F),
	ok.

% cleanup() ->
% 	?debugFmt("Cleanup",[]),
% 	garbage_collect(),
% 	true = code:delete(aqdrv_nif),
% 	true = code:purge(aqdrv_nif),
% 	ok.

async() ->
	?debugFmt("Running many async reads/writes for 20s",[]),
	% ?INIT,
	application:ensure_all_started(crypto),
	ets:new(ops,[set,public,named_table,{write_concurrency,true}]),
	ets:insert(ops,{w,0}),
	ets:insert(ops,{r,0}),

	RandBytes = [crypto:rand_bytes(390) || _ <- lists:seq(1,1000)],
	Pids = [begin
			ets:insert(ops,{P,0}),
		element(1,spawn_monitor(fun() -> w(P,RandBytes) end)) 
	end || P <- lists:seq(1,100)],
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			exit(Reason)
	after 20000 ->
		ok
	end,
	[P ! stop || P <- Pids],
	Ops = rec_counts(0),
	?debugFmt("Ops: ~p",[Ops]).
rec_counts(N) ->
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			rec_counts(N+Reason)
		after 2000 ->
			N
	end.

w(N,RandList) ->
	C = aqdrv:open(N,?LOAD_TEST_COMPR),
	w(N,C,1,RandList,[]).
w(Me,Con,Counter,[Rand|T],L) ->
	{_,QL} = erlang:process_info(self(),message_queue_len),
	ok = aqdrv:stage_map(Con, <<"ITEM1">>, 1, byte_size(Rand)),
	ok = aqdrv:stage_data(Con, Rand),
	{_,_} = aqdrv:stage_flush(Con),
	% WPos = Time = 0,
	{WPos,Time} = aqdrv:write(Con, [<<"WILL BE IGNORED">>], [<<"HEADER">>]),
	case ok of
		_ when QL > 0 ->
			exit(Counter+1);
		% _ when Time > 1000000 ->
		% 	?debugFmt("Time1 ~pms, wpos=~pmb, ~p",[Time div 1000000, WPos div 1000000, Counter]);
		%  Pos rem (1024*1024*1) == 0;
		_ when Me == 1, Counter rem 1000 == 0 ->
			?debugFmt("~pmb, ~p",[WPos div 1000000, Counter]),
			% ?debugFmt("Offset=~p, diffCpy=~p, diffSetup=~p diffAll=~p",[Pos,Diff1,SetupDiff,Diff2]);
			ok;
		_ ->
			ok
	end,
	% ets:update_counter(ops,Me,{2,1}),
	w(Me,Con,Counter+1,T,[Rand|L]);
w(Me,Con,Counter,[],L) ->
	w(Me,Con,Counter,L,[]).

	% ?debugFmt("Verifying",[]),
	% {ok,Fd} = file:open("1",[raw,binary,read]),
	% {ok,Bin} = file:read(Fd,128*1024*1024),
	% WFound = verify(0,Fd,Bin),
	% WFound must be higher, since it is very likely w processes were interrupted
	% while waiting for aqdrv:write.
	% ?debugFmt("Writes found: ~p",[WFound]),
	% true = WFound > element(2,hd(ets:lookup(ops,w))).

% verify(Evs,Fd,<<1,_:4095/binary,Rem/binary>>) ->
% 	verify(Evs+1,Fd,Rem);
% verify(Evs,Fd,<<2,_:4095/binary,Rem/binary>>) ->
% 	verify(Evs,Fd,Rem);
% verify(Evs,Fd,<<0,_:4096/binary,Rem/binary>>) ->
% 	verify(Evs,Fd,Rem);
% verify(Evs,Fd,<<>>) ->
% 	case file:read(Fd,128*1024*1024) of
% 		{ok,Bin} ->
% 			?debugFmt("Next 128mb",[]),
% 			verify(Evs,Fd,Bin);
% 		_ ->
% 			file:close(Fd),
% 			Evs
% 	end;
% verify(Evs,_,<<Wrong,_/binary>>) ->
% 	?debugFmt("Wrong start of page ~p, evs=~p",[Wrong,Evs]),
% 	throw(error).


