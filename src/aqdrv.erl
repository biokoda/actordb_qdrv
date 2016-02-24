-module(aqdrv).
-export([init/1, open/1, stage_map/4, stage_data/2, stage_flush/1, write/3]).

init(Info) when is_map(Info) ->
	aqdrv_nif:init(Info).

open(Hash) ->
	aqdrv_nif:open(Hash).

stage_map({aqdrv,Con}, Name, Type, Size) ->
	aqdrv_nif:stage_map(Con, Name, Type, Size).

stage_data({aqdrv,Con}, <<_/binary>> = Bin) when byte_size(Bin) < 1024*1024*1024 ->
	stage_write1(Con,0, Bin).
% Stage will write at most 256KB at once.
% This way we do not block scheduler for too long.
stage_write1(Con,Offset,Bin) when byte_size(Bin) > Offset ->
	NWritten = aqdrv_nif:stage_data(Con, Bin, Offset),
	stage_write1(Con, Offset + NWritten, Bin);
stage_write1(_,_,_) ->
	ok.

stage_flush({aqdrv,Con}) ->
	aqdrv_nif:stage_flush(Con).

write({aqdrv,Con}, [_|_] = ReplData, [_|_] = Header) ->
	Ref = make_ref(),
	ok = aqdrv_nif:write(Ref, self(),Con, ReplData, Header),
	receive_answer(Ref).

receive_answer(Ref) ->
	receive
		{Ref, Resp} -> Resp
	end.

