-module(test).
-include_lib("eunit/include/eunit.hrl").
-define(CFG,#{threads => 1, startindex => {1}, paths => {"./"}, pwrite => 0}).
-define(INIT,aqdrv:init(?CFG)).

run_test_() ->
	% [file:delete(Fn) || Fn <- ["1"]],
	[
	fun dowrite/0
	% {timeout,50,fun async/0}
	].


dowrite() ->
	?INIT,
	application:ensure_all_started(crypto),
	C = aqdrv:open(1),
	Header = [<<"HEADER_PART1">>,<<"HEADER_PART2">>],
	HeaderSz = iolist_size(Header),
	Body = <<"DATA SECTION START",(crypto:rand_bytes(4096))/binary>>,
	ok = aqdrv:stage_map(C, <<"ITEM1">>, 1, byte_size(Body)),
	ok = aqdrv:stage_data(C, Body),
	{MapSize, DataSize} = aqdrv:stage_flush(C),
	?debugFmt("Mapsize ~p, datasize ~p",[MapSize, DataSize]),
	WPos = aqdrv:write(C, [<<"WILL BE IGNORED">>], Header),
	?debugFmt("Wpos ~p",[WPos]),
	{ok,F} = file:open("1",[read,binary,raw]),
	{ok,Bin} = file:read(F,1024*1024),
	<<(16#184D2A50):32/unsigned-little, HeaderSz:32/unsigned-little,
		"HEADER_PART1","HEADER_PART2",
	  (16#184D2204):32/unsigned-little,_/binary>> = Bin,
	file:close(F),
	ok.

async() ->
	?debugFmt("Running many async reads/writes for 20s",[]),
	?INIT,
	application:ensure_all_started(crypto),
	ets:new(ops,[set,public,named_table,{write_concurrency,true}]),
	ets:insert(ops,{w,0}),
	ets:insert(ops,{r,0}),
	% Random sized buffers
	% +binary:decode_unsigned(crypto:rand_bytes(2)
	RandBytes = [crypto:rand_bytes(4095) || _ <- lists:seq(1,1000)],
	Pids = [element(1,spawn_monitor(fun() -> w(P,RandBytes) end)) || P <- lists:seq(1,512)],
	receive
		{'DOWN',_Monitor,_,_PID,Reason} ->
			exit(Reason)
	after 10000 ->
		ok
	end,
	[exit(P,stop) || P <- Pids],
	timer:sleep(3000),
	?debugFmt("Reads: ~p, Writes: ~p",[ets:lookup(ops,r),ets:lookup(ops,w)]),
	?debugFmt("Verifying",[]),
	{ok,Fd} = file:open("1",[raw,binary,read]),
	{ok,Bin} = file:read(Fd,128*1024*1024),
	WFound = verify(0,Fd,Bin),
	% WFound must be higher, since it is very likely w processes were interrupted
	% while waiting for aqdrv:write.
	?debugFmt("Writes found: ~p",[WFound]),
	true = WFound > element(2,hd(ets:lookup(ops,w))).

verify(Evs,Fd,<<1,_:4095/binary,Rem/binary>>) ->
	verify(Evs+1,Fd,Rem);
verify(Evs,Fd,<<2,_:4095/binary,Rem/binary>>) ->
	verify(Evs,Fd,Rem);
verify(Evs,Fd,<<0,_:4096/binary,Rem/binary>>) ->
	verify(Evs,Fd,Rem);
verify(Evs,Fd,<<>>) ->
	case file:read(Fd,128*1024*1024) of
		{ok,Bin} ->
			?debugFmt("Next 128mb",[]),
			verify(Evs,Fd,Bin);
		_ ->
			file:close(Fd),
			Evs
	end;
verify(Evs,_,<<Wrong,_/binary>>) ->
	?debugFmt("Wrong start of page ~p, evs=~p",[Wrong,Evs]),
	throw(error).


w(N,RandList) ->
	C = aqdrv:open(N),
	w(C,1,RandList,[]).
w(Me,C,[Rand|T],L) ->
	{Pos,Diff1,SetupDiff,Diff2} = aqdrv:write(Me, byte_size(Rand)+1,[<<1>>,Rand]),
	case ok of
		%  Pos rem (1024*1024*1) == 0;
		_ when Diff2 > 100000 ->
			?debugFmt("Offset=~p, diffCpy=~p, diffSetup=~p diffAll=~p",[Pos,Diff1,SetupDiff,Diff2]);
		_ ->
			ok
	end,
	ets:update_counter(ops,w,{2,1}),
	w(Me,C+1,T,[Rand|L]);
w(Me,C,[],L) ->
	w(Me,C,L,[]).