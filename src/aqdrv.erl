-module(aqdrv).
-export([init/1, open/1, stage_write/2, write/3]).

init(Info) when is_map(Info) ->
	aqdrv_nif:init(Info).

open(Hash) ->
	aqdrv_nif:open(Hash).

stage_write({aqdrv,Con}, <<_/binary>> = Bin) ->
	aqdrv_nif:stage_write(Con, Bin).

write({aqdrv,Con}, [_|_] = ReplData, [_|_] = Header) ->
	Ref = make_ref(),
	ok = aqdrv_nif:write(Ref, self(),Con, ReplData, Header),
	receive_answer(Ref).

receive_answer(Ref) ->
	receive
		{Ref, Resp} -> Resp
	end.

