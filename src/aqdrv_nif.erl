-module(aqdrv_nif).
-export([init/1, open/1, stage_map/4,stage_data/3,
	stage_flush/1, write/5, set_tunnel_connector/0, set_thread_fd/4,
	replicate_opts/3]).

open(_) ->
	exit(nif_library_not_loaded).
stage_map(_,_,_,_) ->
	exit(nif_library_not_loaded).
stage_data(_,_,_) ->
	exit(nif_library_not_loaded).
stage_flush(_) ->
	exit(nif_library_not_loaded).
write(_,_,_,_,_) ->
	exit(nif_library_not_loaded).
set_tunnel_connector() ->
	exit(nif_library_not_loaded).
set_thread_fd(_,_,_,_) ->
	exit(nif_library_not_loaded).
replicate_opts(_,_,_) ->
	exit(nif_library_not_loaded).

init(Info) ->
	NifName = "aqdrv_nif",
	NifFileName = case code:priv_dir(aqdrv) of
		{error, bad_name} -> filename:join("priv", NifName);
		Dir -> filename:join(Dir, NifName)
	end,
	case file:read_file_info(NifFileName) of
		{error,_} ->
			FN = filename:join("../priv/",NifName);
		_ ->
			FN = NifFileName
	end,
	case erlang:load_nif(FN, Info#{logname => "drv_"++hd(string:tokens(atom_to_list(node()),"@"))++".txt"}) of
		ok ->
			ok;
		{error,{upgrade,_}} ->
			ok;
		{error,{reload,_}} ->
			ok
	end.
